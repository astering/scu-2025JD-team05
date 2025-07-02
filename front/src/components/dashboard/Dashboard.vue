<!-- 驾驶舱:BI报表界面 -->
<template>
  <div class="flex h-screen bg-gray-900 text-white">
    <!-- 左侧导航栏 -->
    <aside class="w-64 bg-gray-800 p-6">
      <h2 class="text-green-400 text-lg font-bold mb-6">BI 报表</h2>
      <nav class="space-y-3">
        <button
          v-for="(report, index) in reports"
          :key="index"
          @click="current = report.component"
          class="w-full text-left px-3 py-2 rounded hover:bg-gray-700 transition"
          :class="{ 'bg-gray-700': current === report.component }"
        >
          {{ report.name }}
        </button>
      </nav>
    </aside>

    <!-- 主体内容区 -->
    <main class="flex-1 p-6 overflow-auto">
      <component :is="current" />
    </main>
  </div>
</template>

<script setup>
import { ref } from 'vue'

// 导入子报表组件
import DecadePreference from '@/components/dashboard/DecadePreference.vue'
import ActiveHours from '@/components/dashboard/ActiveHours.vue'
import YearlyTrend from '@/components/dashboard/YearlyTrend.vue'
import YearlyCount from "@/components/dashboard/YearlyCount.vue"
import ArtistAttribution from "@/components/dashboard/ArtistAttribution.vue";
import UserAttribution from "@/components/dashboard/UserAttribution.vue";

const reports = [
  { name: '不同年代听众偏好类型变化', component: DecadePreference },
  { name: '听歌活跃时间段分析', component: ActiveHours },
  { name: '年度歌曲风格变化趋势', component: YearlyTrend },
  { name: '年度歌曲上传量变化趋势', component: YearlyCount },
  { name: '歌手地区分布', component: ArtistAttribution },
  { name: '用户分布热力图',component: UserAttribution }
]

const current = ref(DecadePreference)
</script>
