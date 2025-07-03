<template>
  <div class="p-6 max-w-3xl mx-auto text-white">
    <h1 class="text-3xl font-bold mb-6 text-center">播放量排行榜（前10）</h1>
    <ul class="space-y-4">
      <li
        v-for="(track, index) in ranks"
        :key="track.id"
        class="p-4 bg-gradient-to-r from-purple-900 to-gray-800 rounded-xl hover:shadow-xl transition duration-300"
      >
        <div class="flex items-center space-x-4">
          <div class="text-2xl font-bold text-yellow-400 w-8 text-center">
            {{ index + 1 }}
          </div>
          <div class="flex-1">
            <div class="text-lg font-semibold">
              {{ extractTrackName(track.title) }} -- {{ extractArtistName(track.title) }}
            </div>
            <div class="text-sm text-gray-300">
              {{ decodeURIComponent(track.artist).replace(/\+/g, ' ') }} · {{ track.playcount }} 次播放
            </div>
          </div>
        </div>
      </li>
    </ul>
  </div>
</template>

<script setup>
import { ref, onMounted } from 'vue'

const ranks = ref([])

onMounted(async () => {
  const res = await fetch('/api/music/rank')
  ranks.value = await res.json()
})

// 提取歌曲名（“/_/”后部分）并将 + 替换为空格
const extractTrackName = (title) => {
  try {
    const parts = title.split('/_/')
    return decodeURIComponent(parts[1] || title).replace(/\+/g, ' ')
  } catch {
    return decodeURIComponent(title).replace(/\+/g, ' ')
  }
}

// 提取艺人名（“/_/”前部分）并将 + 替换为空格
const extractArtistName = (title) => {
  try {
    const parts = title.split('/_/')
    return decodeURIComponent(parts[0] || "").replace(/\+/g, ' ')
  } catch {
    return ""
  }
}
</script>
