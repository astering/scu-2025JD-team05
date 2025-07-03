<template>
  <div v-if="shouldShow" id="waifu" class="live2d-container"></div>
</template>

<script setup>
import { onMounted, ref } from 'vue'
import { useRoute } from 'vue-router'

const shouldShow = ref(false)
const route = useRoute()

/**
 * 动态异步加载 JS 或 CSS 资源
 */
function loadExternalResource(url, type) {
  return new Promise((resolve, reject) => {
    const tag = type === 'css' ? document.createElement('link') : document.createElement('script')
    if (type === 'css') {
      tag.rel = 'stylesheet'
      tag.href = url
    } else {
      tag.src = url
    }
    tag.onload = () => resolve(url)
    tag.onerror = () => reject(new Error(`加载失败: ${url}`))
    document.head.appendChild(tag)
  })
}

onMounted(async () => {
  // 1. 屏幕宽度判断：移动端不显示
  if (window.innerWidth < 768) return

  // 2. 登录判断（可根据实际 token 机制调整）
  const isLoggedIn = localStorage.getItem("token") !== null
  if (!isLoggedIn) return

  // 3. 限定路径（只在首页或 /home 显示）
  const allowedPaths = ['/', '/home']
  if (!allowedPaths.includes(route.path)) return

  // 4. 显示容器
  shouldShow.value = true

  // 5. 静态资源路径（FastAPI 提供）
  const live2d_path = "http://127.0.0.1:8000/assets/"
  const api_path = live2d_path + "api/" // 用于模型、贴图请求

  try {
    // 加载所需资源
    await Promise.all([
      loadExternalResource(live2d_path + "waifu.css", "css"),
      loadExternalResource(live2d_path + "live2d.min.js", "js"),
      loadExternalResource(live2d_path + "waifu-tips.js", "js"),
    ])

    // 初始化 Live2D，看板娘出现！
    window.initWidget?.({
      waifuPath: live2d_path + "waifu-tips.json",
      cdnPath: api_path,
      tools: ["hitokoto", "switch-model", "switch-texture", "quit"],
    })
  } catch (error) {
    console.error("Live2D 加载失败：", error)
  }
})
</script>

<style scoped>
.live2d-container {
  position: fixed;
  right: 0;
  bottom: 0;
  z-index: 999;
}
</style>
