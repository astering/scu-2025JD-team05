<template>
  <div class="yearly-count">
    <h2>歌手地区分布</h2>
    <div class="tableauPlaceholder" id="viz1751504748426" style="position: relative">
      <noscript>
        <a href="#">
          <img alt="工作表 1 "
            src="https:&#47;&#47;public.tableau.com&#47;static&#47;images&#47;te&#47;test_17513319001510&#47;1&#47;1_rss.png"
            style="border: none" />
        </a>
      </noscript>
      <object class="tableauViz" style="display: none">
        <param name="host_url" value="https%3A%2F%2Fpublic.tableau.com%2F" />
        <param name="embed_code_version" value="3" />
        <param name="site_root" value="" />
        <param name="name" value="test_17513319001510&#47;1" />
        <param name="tabs" value="no" />
        <param name="toolbar" value="yes" />
        <param name="static_image"
          value="https:&#47;&#47;public.tableau.com&#47;static&#47;images&#47;te&#47;test_17513319001510&#47;1&#47;1.png" />
        <param name="animate_transition" value="yes" />
        <param name="display_static_image" value="yes" />
        <param name="display_spinner" value="yes" />
        <param name="display_overlay" value="yes" />
        <param name="display_count" value="yes" />
        <param name="language" value="en-US" />
      </object>
    </div>
  </div>
</template>

<script setup lang="ts">
import { onMounted, ref } from 'vue';

const vizElementRef = ref(null);

onMounted(() => {
  const divElement = document.getElementById("viz1751504748426");
  const vizElement = divElement?.getElementsByTagName("object")[0];
  if (vizElement) {
    vizElement.style.width = "100%";
    vizElement.style.height = `${divElement.offsetWidth * 0.75}px`;
    const scriptElement = document.createElement("script");
    scriptElement.src = "https://public.tableau.com/javascripts/api/viz_v1.js";
    vizElement.parentNode.insertBefore(scriptElement, vizElement);
  }
});
</script>

<style scoped>
.yearly-count {
  text-align: center;
  margin-top: 20px;
}

.yearly-count img {
  max-width: 100%;
  height: auto;
  border: 1px solid #ccc;
  border-radius: 8px;
}
</style>
