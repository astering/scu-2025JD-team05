<script setup lang="ts">
</script>

<template>
  <div class="artist-distribution">
    <h2 class="text-xl font-bold mb-4 text-center">歌手地区分布</h2>

    <!-- 使用 iframe 嵌入 HTML 地图 -->
    <iframe
      src="/charts/artist_distribution_world_map.html"
      class="w-full h-[600px] border rounded shadow"
    />
  </div>
</template>

<style scoped>
.artist-distribution {
  text-align: center;
  margin-top: 20px;
}
</style>
