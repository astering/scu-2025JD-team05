<script setup lang="ts">
import { onMounted, ref } from 'vue'

const iframeLoaded = ref(false)

onMounted(() => {
  iframeLoaded.value = true
})
</script>

<template>
  <div class="heatmap-container">
    <h2 class="text-xl font-bold mb-4">用户国家热力图</h2>
    <iframe
      v-if="iframeLoaded"
      src="/charts/user_country_heatmap.html"
      frameborder="0"
      width="100%"
      height="600"
      class="rounded-lg shadow-md"
    ></iframe>
  </div>
</template>

<style scoped>
.heatmap-container {
  padding: 20px;
}
</style>
