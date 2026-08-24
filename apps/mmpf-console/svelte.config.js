import adapter from "@sveltejs/adapter-static";

const base = process.env.MMPF_CONSOLE_BASE_PATH ?? "";
if (base !== "" && (!base.startsWith("/") || base.endsWith("/"))) {
  throw new Error("MMPF_CONSOLE_BASE_PATH must be empty or start with / without a trailing /");
}

export default {
  kit: {
    adapter: adapter({
      fallback: "index.html",
    }),
    paths: { base },
  },
};
