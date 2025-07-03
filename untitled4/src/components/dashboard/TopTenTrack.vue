
<script setup lang="ts">
import { ref, onMounted } from 'vue'
import {
  Chart as ChartJS,
  Title,
  Tooltip,
  Legend,
  BarElement,
  CategoryScale,
  LinearScale
} from 'chart.js'
import { Bar } from 'vue-chartjs'

ChartJS.register(Title, Tooltip, Legend, BarElement, CategoryScale, LinearScale)

const chartData = ref({
  labels: [],
  datasets: []
})

onMounted(async () => {
  const res = await fetch('/top_track.json')
  const json = await res.json()
  const topTracks = json.result[0].data.slice(0, 10)

  chartData.value.labels = topTracks.map(item =>
    decodeURIComponent(item.title.replace(/\+/g, ' '))
  )
  chartData.value.datasets = [
    {
      label: '播放量',
      backgroundColor: 'rgba(34,197,94,0.7)',
      data: topTracks.map(item => item['MIN(playcount)'])
    }
  ]
})
</script>

<template>
  <div class="p-4 bg-white rounded shadow">
    <h2 class="text-xl font-bold text-center mb-4">播放量前十的曲目</h2>
    <Bar
      v-if="chartData.labels.length"
      :data="chartData"
      :options="{
        responsive: true,
        maintainAspectRatio: false,
        plugins: {
          legend: { display: false },
          tooltip: { enabled: true }
        },
        scales: {
          x: {
            ticks: { color: '#374151' },
            title: { display: true, text: '曲目', color: '#111827' }
          },
          y: {
            ticks: { color: '#374151' },
            title: { display: true, text: '播放量', color: '#111827' }
          }
        }
      }"
      style="height: 400px"
    />
  </div>
</template>

<style scoped>
canvas {
  max-width: 100%;
}
</style>