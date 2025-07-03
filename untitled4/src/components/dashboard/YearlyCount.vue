<script setup lang="ts">
</script>

<template>
  <div class="yearly-count">
    <h2>歌曲数量按年份统计</h2>
    <iframe
      src="/charts/tracks_per_year_trend.html"
      class="w-full h-[600px] border rounded"
      style="min-height: 600px;"
      frameborder="0"
    ></iframe>
  </div>
</template>

<style scoped>
.yearly-count {
  text-align: center;
  margin-top: 20px;
}

/* iframe 自适应宽度和固定高度 */
.yearly-count iframe {
  width: 100%;
  height: 600px;
  border: 1px solid #ccc;
  border-radius: 8px;
}
</style>
