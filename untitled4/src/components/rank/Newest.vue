<template>
  <div class="p-4">
    <h1 class="text-2xl font-bold mb-4">最新上线的音乐</h1>
    <ul>
      <li v-for="song in songs" :key="song.id">
         {{ song.title }} - {{ song.artist }}（{{ song.release_date }}）
      </li>
    </ul>
  </div>
</template>

<script setup>
import { ref, onMounted } from 'vue'

const songs = ref([])

onMounted(async () => {
  const res = await fetch('/api/new_music')
  songs.value = await res.json()
})
</script>
