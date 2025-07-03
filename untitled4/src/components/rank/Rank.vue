<template>
  <div class="p-4">
    <h1 class="text-2xl font-bold mb-4"> 播放量排行榜（前10）</h1>
    <ul>
      <li v-for="track in ranks" :key="track.id">
        {{ track.title }} - {{ track.artist }}（{{ track.playcount }} 次播放）
      </li>
    </ul>
  </div>
</template>

<script setup>
import { ref, onMounted } from 'vue'

const ranks = ref([])

onMounted(async () => {
  const res = await fetch('/api/music/rank') // 接口路径已匹配后端
  ranks.value = await res.json()
})
</script>
