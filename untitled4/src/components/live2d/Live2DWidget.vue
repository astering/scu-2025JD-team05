<script setup>
import { onMounted, ref, computed } from 'vue'
import { useRoute } from 'vue-router'

const shouldShow = ref(false)
const route = useRoute()

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
  if (window.innerWidth < 768) return // ÒÆ¶¯¶Ë²»ÏÔÊ¾
  const isLoggedIn = localStorage.getItem("token") !== null
  if (!isLoggedIn) return

  const currentPath = route.path
  if (currentPath !== '/' && currentPath !== '/home') return

  shouldShow.value = true

  const live2d_path = "https://fastly.jsdelivr.net/gh/stevenjoezhang/live2d-widget@latest/"
  await Promise.all([
    loadExternalResource(live2d_path + "waifu.css", "css"),
    loadExternalResource(live2d_path + "live2d.min.js", "js"),
    loadExternalResource(live2d_path + "waifu-tips.js", "js"),
  ])
  window.initWidget?.({
    waifuPath: live2d_path + "waifu-tips.json",
    cdnPath: "https://unpkg.com/live2d-widget-model-shizuku/assets/",
    model: "shizuku",
    tools: ["hitokoto", "switch-model", "switch-texture", "quit"],
  })
})
</script>
