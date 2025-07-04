<script setup lang="ts">
import { onMounted, ref } from 'vue'
import { useRoute } from 'vue-router'
import axios from 'axios'

const route = useRoute()
const query = route.query.q as string
const results = ref([])

onMounted(() => {
  if (query) {
    axios.get('/api/similarity/search', { params: { q: query } })
      .then(res => {
        results.value = res.data?.results || []
      })
      .catch(err => {
        console.error('搜索失败:', err)
      })
  }
})
</script>

<template>
  <div class="min-h-screen bg-slate-900 text-white p-6">
    <!-- 返回按钮 -->
    <router-link
      to="/home"
      class="inline-block mb-4 px-4 py-2 bg-slate-700 text-white rounded-lg hover:bg-slate-600 transition"
    >
      ← 返回主页
    </router-link>

    <h1 class="text-3xl font-bold mb-6">搜索结果: "{{ query }}"</h1>

    <ul v-if="results.length" class="space-y-4">
      <li
        v-for="(item, index) in results"
        :key="index"
        class="p-4 bg-slate-800 rounded-xl shadow hover:bg-slate-700 transition"
      >
        <p class="text-xl font-semibold">{{ item.track_name }}</p>
        <p class="text-sm text-gray-300">
          歌手: {{ item.artist_name }} | 相似度: {{ item.similar_score.toFixed(4) }}
        </p>
      </li>
    </ul>

    <p v-else class="text-gray-400">未找到相关歌曲。</p>
  </div>
</template>
