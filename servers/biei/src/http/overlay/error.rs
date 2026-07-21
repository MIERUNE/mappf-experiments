use super::{MAX_GEOJSON_COORDINATES, MAX_GEOJSON_FEATURES};
use biei_core::types::MAX_STATIC_OVERLAYS;

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum OverlayParseError {
    Empty,
    InvalidPathSyntax,
    InvalidStrokeWidth,
    InvalidColor,
    InvalidOpacity,
    InvalidPercentEncoding,
    InvalidByte { byte: u8, index: usize },
    Truncated,
    CoordinateOverflow,
    CoordinateOutOfRange,
    TooManyPoints,
    TooManyOverlays,
    InvalidGeoJsonSyntax,
    UnsupportedGeoJsonType,
    TooManyFeatures,
    TooManyCoordinates,
    InvalidPinSyntax,
    InvalidPinSize,
    InvalidPinLabel,
}

impl std::fmt::Display for OverlayParseError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Empty => write!(f, "polyline must not be empty"),
            Self::InvalidPathSyntax => write!(f, "path overlay must be path-...(<polyline>)"),
            Self::InvalidStrokeWidth => write!(f, "path stroke width must be positive"),
            Self::InvalidColor => write!(f, "path color must be a 3- or 6-digit hex color"),
            Self::InvalidOpacity => write!(f, "path opacity must be between 0 and 1"),
            Self::InvalidPercentEncoding => write!(f, "invalid percent-encoded polyline"),
            Self::InvalidByte { byte, index } => {
                write!(f, "invalid polyline byte {byte} at index {index}")
            }
            Self::Truncated => write!(f, "polyline ended in the middle of a coordinate"),
            Self::CoordinateOverflow => write!(f, "polyline coordinate overflow"),
            Self::CoordinateOutOfRange => write!(f, "polyline coordinate is out of range"),
            Self::TooManyPoints => write!(f, "path overlay has too many points"),
            Self::TooManyOverlays => {
                write!(f, "request has more than {MAX_STATIC_OVERLAYS} overlays")
            }
            Self::InvalidGeoJsonSyntax => write!(f, "geojson overlay payload is not valid JSON"),
            Self::UnsupportedGeoJsonType => {
                write!(f, "geojson overlay must be a Feature or FeatureCollection")
            }
            Self::TooManyFeatures => write!(
                f,
                "geojson overlay has more than {MAX_GEOJSON_FEATURES} features"
            ),
            Self::TooManyCoordinates => write!(
                f,
                "geojson overlay has more than {MAX_GEOJSON_COORDINATES} coordinates"
            ),
            Self::InvalidPinSyntax => {
                write!(
                    f,
                    "pin overlay must be pin-{{s|m|l}}[-label]+color(lon,lat)"
                )
            }
            Self::InvalidPinSize => write!(f, "pin size must be s, m, or l"),
            Self::InvalidPinLabel => write!(f, "pin label must be 1 ASCII alphanumeric character"),
        }
    }
}

impl std::error::Error for OverlayParseError {}
