<template>
  <div class="container mx-auto px-4 py-8">
    <div class="flex justify-between items-center mb-6">
      <h1 class="text-3xl font-bold text-white">Top100 热门歌手</h1>
      <router-link
        to="/home"
        class="text-sm text-blue-400 hover:text-blue-300 transition"
      >
        返回首页 <i class="fas fa-arrow-right ml-1"></i>
      </router-link>
    </div>

    <ul class="grid grid-cols-1 sm:grid-cols-2 md:grid-cols-3 gap-4">
      <li
        v-for="(artist, index) in artists"
        :key="artist.id"
        class="bg-white/5 border border-white/10 backdrop-blur-md p-4 rounded-xl shadow hover:bg-white/10 transition cursor-pointer"
      >
        <div class="text-lg font-semibold text-white">
          {{ index + 1 }}. {{ artist.name.replace(/\+/g, ' ') }}
        </div>
        <div class="text-sm text-gray-300">
          播放量: {{ artist.playcount.toLocaleString() }}
        </div>
      </li>
    </ul>
  </div>
</template>

<script setup>
import { ref, onMounted } from 'vue'
import axios from 'axios'

const artists = ref([])

onMounted(async () => {
  try {
    const res = await axios.get('http://127.0.0.1:8000/api/artist/top-artists', {
      params: { limit: 100 }
    })
    artists.value = res.data
  } catch (error) {
    console.error("获取前100歌手失败", error)
  }
})
</script>
