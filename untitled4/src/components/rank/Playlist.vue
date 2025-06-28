<template>
  <div class="p-4">
    <h1 class="text-2xl font-bold mb-4">推荐歌单</h1>
    <ul>
      <li v-for="pl in playlists" :key="pl.id">{{ pl.title }} ({{ pl.num_tracks }} 首)</li>
    </ul>
  </div>
</template>

<script setup>
import { ref, onMounted } from 'vue'

const playlists = ref([])

onMounted(async () => {
  const res = await fetch('/api/playlist')
  playlists.value = await res.json()
})
</script>