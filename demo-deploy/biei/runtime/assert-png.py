#!/usr/bin/env python3
"""Small dependency-free PNG contract checker for the runtime E2E.

Biei's PNG encoder emits non-interlaced 8-bit RGBA images. Keeping this checker
to that exact production contract avoids installing Pillow in an already
expensive container-build job while still validating rendered pixels rather
than only the PNG signature.
"""

from __future__ import annotations

import argparse
import struct
import sys
import zlib
from pathlib import Path

PNG_SIGNATURE = b"\x89PNG\r\n\x1a\n"
RGBA_CHANNELS = 4


def paeth(left: int, above: int, upper_left: int) -> int:
    estimate = left + above - upper_left
    distance_left = abs(estimate - left)
    distance_above = abs(estimate - above)
    distance_upper_left = abs(estimate - upper_left)
    if distance_left <= distance_above and distance_left <= distance_upper_left:
        return left
    if distance_above <= distance_upper_left:
        return above
    return upper_left


def decode_rgba(path: Path) -> tuple[int, int, bytes]:
    encoded = path.read_bytes()
    if not encoded.startswith(PNG_SIGNATURE):
        raise ValueError(f"{path}: not a PNG")

    width = height = None
    idat = bytearray()
    saw_iend = False
    offset = len(PNG_SIGNATURE)
    while offset < len(encoded):
        if offset + 12 > len(encoded):
            raise ValueError(f"{path}: truncated PNG chunk")
        length = struct.unpack_from(">I", encoded, offset)[0]
        kind = encoded[offset + 4 : offset + 8]
        data_start = offset + 8
        data_end = data_start + length
        crc_end = data_end + 4
        if crc_end > len(encoded):
            raise ValueError(f"{path}: truncated {kind!r} chunk")
        data = encoded[data_start:data_end]
        # Verify the chunk checksum. Without this the checker accepts a
        # structurally corrupt file whose bytes merely happen to decompress,
        # which would let a broken encoder pass as valid output.
        stored_crc = struct.unpack_from(">I", encoded, data_end)[0]
        actual_crc = zlib.crc32(kind + data) & 0xFFFFFFFF
        if stored_crc != actual_crc:
            raise ValueError(
                f"{path}: {kind.decode('ascii', 'replace')} chunk CRC "
                f"{stored_crc:#010x} != computed {actual_crc:#010x}"
            )
        if kind == b"IHDR":
            if length != 13:
                raise ValueError(f"{path}: invalid IHDR length")
            width, height, depth, color, compression, filtering, interlace = struct.unpack(
                ">IIBBBBB", data
            )
            if (depth, color, compression, filtering, interlace) != (8, 6, 0, 0, 0):
                raise ValueError(
                    f"{path}: expected non-interlaced RGBA8 PNG, got "
                    f"depth={depth} color={color} compression={compression} "
                    f"filter={filtering} interlace={interlace}"
                )
        elif kind == b"IDAT":
            idat.extend(data)
        elif kind == b"IEND":
            saw_iend = True
            break
        offset = crc_end

    if width is None or height is None or not idat:
        raise ValueError(f"{path}: missing IHDR or IDAT")
    # A PNG without a terminating IEND is truncated, even when every chunk
    # present decodes cleanly.
    if not saw_iend:
        raise ValueError(f"{path}: missing IEND chunk")

    filtered = zlib.decompress(idat)
    stride = width * RGBA_CHANNELS
    expected = height * (stride + 1)
    if len(filtered) != expected:
        raise ValueError(
            f"{path}: decoded stream has {len(filtered)} bytes, expected {expected}"
        )

    pixels = bytearray(height * stride)
    previous = bytearray(stride)
    input_offset = 0
    for y in range(height):
        filter_kind = filtered[input_offset]
        input_offset += 1
        source = filtered[input_offset : input_offset + stride]
        input_offset += stride
        row = bytearray(stride)
        for index, value in enumerate(source):
            left = row[index - RGBA_CHANNELS] if index >= RGBA_CHANNELS else 0
            above = previous[index]
            upper_left = previous[index - RGBA_CHANNELS] if index >= RGBA_CHANNELS else 0
            if filter_kind == 0:
                predictor = 0
            elif filter_kind == 1:
                predictor = left
            elif filter_kind == 2:
                predictor = above
            elif filter_kind == 3:
                predictor = (left + above) // 2
            elif filter_kind == 4:
                predictor = paeth(left, above, upper_left)
            else:
                raise ValueError(f"{path}: unsupported PNG filter {filter_kind}")
            row[index] = (value + predictor) & 0xFF
        start = y * stride
        pixels[start : start + stride] = row
        previous = row
    return width, height, bytes(pixels)


def parse_color(value: str) -> tuple[int, int, int, int]:
    value = value.removeprefix("#")
    if len(value) == 6:
        value += "ff"
    if len(value) != 8:
        raise argparse.ArgumentTypeError("color must be RRGGBB or RRGGBBAA")
    try:
        return tuple(bytes.fromhex(value))  # type: ignore[return-value]
    except ValueError as error:
        raise argparse.ArgumentTypeError("color must be hexadecimal") from error


def pixel_at(pixels: bytes, width: int, x: int, y: int) -> tuple[int, int, int, int]:
    if not (0 <= x < width and y >= 0):
        raise ValueError(f"sample ({x}, {y}) lies outside the image")
    index = (y * width + x) * RGBA_CHANNELS
    if index + RGBA_CHANNELS > len(pixels):
        raise ValueError(f"sample ({x}, {y}) lies outside the image")
    return tuple(pixels[index : index + RGBA_CHANNELS])  # type: ignore[return-value]


def close(actual: tuple[int, ...], expected: tuple[int, ...], tolerance: int) -> bool:
    return all(abs(a - b) <= tolerance for a, b in zip(actual, expected, strict=True))


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("image", type=Path)
    parser.add_argument("--size", required=True, help="expected WIDTHxHEIGHT")
    parser.add_argument(
        "--sample",
        action="append",
        default=[],
        metavar="X,Y,COLOR",
        help="require one pixel to match COLOR",
    )
    parser.add_argument(
        "--min-color",
        action="append",
        default=[],
        metavar="COLOR,COUNT",
        help="require at least COUNT pixels matching COLOR",
    )
    parser.add_argument(
        "--different-from",
        type=Path,
        help="require a minimum number of pixels to differ from another PNG",
    )
    parser.add_argument("--min-different", type=int, default=1)
    parser.add_argument("--tolerance", type=int, default=3)
    args = parser.parse_args()

    try:
        expected_width, expected_height = (int(part) for part in args.size.split("x", 1))
        width, height, pixels = decode_rgba(args.image)
        if (width, height) != (expected_width, expected_height):
            raise ValueError(
                f"{args.image}: dimensions {(width, height)} != "
                f"{(expected_width, expected_height)}"
            )

        for value in args.sample:
            x_text, y_text, color_text = value.split(",", 2)
            x, y = int(x_text), int(y_text)
            expected = parse_color(color_text)
            actual = pixel_at(pixels, width, x, y)
            if not close(actual, expected, args.tolerance):
                raise ValueError(
                    f"{args.image}: pixel ({x}, {y}) is {actual}, expected {expected} "
                    f"within tolerance {args.tolerance}"
                )

        image_pixels = [
            tuple(pixels[index : index + RGBA_CHANNELS])
            for index in range(0, len(pixels), RGBA_CHANNELS)
        ]
        for value in args.min_color:
            color_text, count_text = value.rsplit(",", 1)
            expected = parse_color(color_text)
            required = int(count_text)
            actual = sum(
                close(pixel, expected, args.tolerance) for pixel in image_pixels
            )
            if actual < required:
                raise ValueError(
                    f"{args.image}: only {actual} pixels match {expected}, "
                    f"expected at least {required}"
                )

        if args.different_from is not None:
            other_width, other_height, other = decode_rgba(args.different_from)
            if (other_width, other_height) != (width, height):
                raise ValueError(
                    f"{args.image} and {args.different_from} have different dimensions"
                )
            different = sum(
                not close(
                    tuple(pixels[index : index + RGBA_CHANNELS]),
                    tuple(other[index : index + RGBA_CHANNELS]),
                    args.tolerance,
                )
                for index in range(0, len(pixels), RGBA_CHANNELS)
            )
            if different < args.min_different:
                raise ValueError(
                    f"{args.image}: only {different} pixels differ from "
                    f"{args.different_from}, expected at least {args.min_different}"
                )
    except (OSError, ValueError, zlib.error) as error:
        print(error, file=sys.stderr)
        return 1

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
