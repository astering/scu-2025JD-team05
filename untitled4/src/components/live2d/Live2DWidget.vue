<template>
  <div v-if="shouldShow" id="waifu-container"></div>
</template>

<script setup>
import { onMounted, ref } from 'vue'

const shouldShow = ref(false)

function loadExternalResource(url, type) {
  return new Promise((resolve, reject) => {
    const tag = type === "css" ? document.createElement("link") : document.createElement("script")
    if (type === "css") {
      tag.rel = "stylesheet"
      tag.href = url
    } else {
      tag.src = url
    }
    tag.onload = () => resolve(url)
    tag.onerror = () => reject(url)
    document.head.appendChild(tag)
  })
}

onMounted(async () => {
  if (window.innerWidth < 768) return // 移动端不显示
  const isLoggedIn = localStorage.getItem("token") !== null
  if (!isLoggedIn) return

  shouldShow.value = true

  const live2d_path = "https://fastly.jsdelivr.net/gh/stevenjoezhang/live2d-widget@latest/"

  await Promise.all([
    loadExternalResource(live2d_path + "waifu.css", "css"),
    loadExternalResource(live2d_path + "live2d.min.js", "js"),
    loadExternalResource(live2d_path + "waifu-tips.js", "js"),
  ])

  window.initWidget?.({
    waifuPath: live2d_path + "waifu-tips.json",
    cdnPath: "https://unpkg.com/live2d-widget-model-shizuku/assets/", // 本地模型根目录，确保该目录下有model文件夹和index.json
    model:"shizuku",
    tools: ["hitokoto", "switch-model", "switch-texture", "quit"],
  })
})
</script>

<style scoped>
#waifu-container {
  position: fixed;
  right: 0;
  bottom: 0;
  z-index: 99999;
}
</style>
