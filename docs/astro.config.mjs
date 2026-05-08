// @ts-check
import { defineConfig } from "astro/config";
import starlight from "@astrojs/starlight";

export default defineConfig({
  integrations: [
    starlight({
      title: "ldrs",
      description:
        "Fast, minimal-allocation Rust binary for the E&L of ETL. Arrow RecordBatches end-to-end.",
      customCss: ["./src/styles/custom.css"],
      components: {
        SiteTitle: "./src/components/SiteTitle.astro",
        Hero: "./src/components/HeroWordmark.astro",
      },
      social: [
        {
          icon: "github",
          label: "GitHub",
          href: "https://github.com/johanan/ldrs",
        },
      ],
      sidebar: [
        {
          label: "Introduction",
          items: [
            { label: "What is ldrs?", slug: "index" },
            { label: "Getting started", slug: "getting-started" },
            { label: "Vision", slug: "vision" },
            { label: "ldrs-sf", slug: "ldrs-sf" },
          ],
        },
      ],
    }),
  ],
});
