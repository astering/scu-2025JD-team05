<template>
  <div>
    <h2 class="text-xl font-bold mb-4">听歌活跃时间段分析</h2>

    <div class="mb-4 flex gap-4">
      <button
        class="px-4 py-2 rounded bg-blue-500 text-white hover:bg-blue-600"
        :class="{ 'bg-blue-700': selected === 'hour' }"
        @click="selected = 'hour'"
      >
        按小时
      </button>
      <button
        class="px-4 py-2 rounded bg-blue-500 text-white hover:bg-blue-600"
        :class="{ 'bg-blue-700': selected === 'weekday' }"
        @click="selected = 'weekday'"
      >
        按周几
      </button>
      <button
        class="px-4 py-2 rounded bg-blue-500 text-white hover:bg-blue-600"
        :class="{ 'bg-blue-700': selected === 'month' }"
        @click="selected = 'month'"
      >
        按月
      </button>
    </div>

    <iframe
      v-if="selected === 'hour'"
      src="/charts/play_event_by_hour.html"
      class="w-full h-[600px] border"
    />
    <iframe
      v-else-if="selected === 'weekday'"
      src="/charts/play_event_by_weekday.html"
      class="w-full h-[600px] border"
    />
    <iframe
      v-else
      src="/charts/play_event_by_month.html"
      class="w-full h-[600px] border"
    />
  </div>
</template>

<script setup lang="ts">
import { ref } from 'vue'
const selected = ref('hour')
</script>
