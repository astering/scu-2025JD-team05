<template>
  <div>
    <h2 class="text-xl font-bold mb-4">不同年代听众偏好类型变化</h2>

    <!-- 下拉菜单选择图表类型 -->
    <div class="mb-4">
      <label for="chartType" class="mr-2 text-white">选择图表：</label>
      <select
          id="chartType"
          v-model="selectedChart"
          class="border border-slate-600 rounded px-3 py-1 bg-slate-800 text-white focus:outline-none focus:ring-2 focus:ring-blue-400"
      >
        <option class="bg-slate-800 text-white" value="heatmap">热力图</option>
       <option class="bg-slate-800 text-white" value="bar">柱状图</option>
      </select>
    </div>


    <!-- 根据选择显示图表 -->
    <iframe
      :src="chartSrc"
      class="w-full h-[600px] border rounded shadow"
    />
  </div>
</template>

<script setup lang="ts">
import { ref, computed } from 'vue'

// 当前选择的图表类型
const selectedChart = ref('heatmap')

// 根据选择返回 iframe src
const chartSrc = computed(() => {
  if (selectedChart.value === 'bar') {
    return '/charts/top10_genres_by_decade_stacked_bar.html'
  }
  return '/charts/top10_genres_by_decade_heatmap.html'
})
</script>
