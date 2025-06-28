<template>
  <div class="p-4">
    <h1 class="text-2xl font-bold mb-4">热门歌手</h1>
    <ul>
      <li v-for="artist in artists" :key="artist.id">
        {{ artist.name }}（{{ artist.num_tracks }} 首歌）
      </li>
    </ul>
  </div>
</template>

<script setup>
import { ref, onMounted } from 'vue'

const artists = ref([])

onMounted(async () => {
  const res = await fetch('/api/artists')
  artists.value = await res.json()
})
</script>
