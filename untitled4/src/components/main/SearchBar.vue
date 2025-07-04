<script setup lang="ts">
import { ref } from 'vue'
import { useRouter } from 'vue-router'

const keyword = ref('')
const router = useRouter()

function handleSearch() {
  if (!keyword.value.trim()) {
    alert('关键词为空！')
    return
  }
  router.push({ name: 'search', query: { q: keyword.value.trim() } })
}

function onKeydown(event: KeyboardEvent) {
  if (event.key === 'Enter') {
    handleSearch()
  }
}
</script>

<template>
  <div class="flex items-center gap-2">
    <input
      v-model="keyword"
      type="text"
      placeholder="输入歌曲名称..."
      class="rounded-xl p-2 px-4 w-72 text-black shadow focus:outline-none focus:ring-2 focus:ring-blue-400 transition-all duration-200"
      @keydown="onKeydown"
    />
    <button
      @click="handleSearch"
      class="bg-blue-500 text-white px-4 py-2 rounded-xl hover:bg-blue-600 transition-all duration-200"
    >
      搜索
    </button>
  </div>
</template>
