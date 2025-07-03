<!-- 驾驶舱:BI报表界面 -->
<template>
  <div class="flex h-screen bg-white text-gray-900">
    <!-- 左侧导航栏 -->
    <aside class="w-64 bg-gray-100 p-6 border-r border-gray-300">
      <h2 class="text-blue-600 text-lg font-bold mb-6">BI 报表</h2>
      <router-link to="/home" class="text-sm bg-blue-500 hover:bg-blue-600 text-white px-3 py-1 rounded transition">
          返回
        </router-link>
      <nav class="space-y-3">
        <button
          v-for="(report, index) in reports"
          :key="index"
          @click="current = report.component"
          class="w-full text-left px-3 py-2 rounded hover:bg-blue-100 transition"
          :class="{ 'bg-blue-200 font-semibold': current === report.component }"
        >
          {{ report.name }}
        </button>
      </nav>
    </aside>

    <!-- 主体内容区 -->
    <main class="flex-1 p-6 overflow-auto bg-white">
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
import LLM from '@/components/dashboard/LLM.vue';

const reports = [
  { name: '不同年代听众偏好类型变化', component: DecadePreference },
  { name: '听歌活跃时间段分析', component: ActiveHours },
  { name: '年度歌曲风格变化趋势', component: YearlyTrend },
  { name: '年度歌曲上传量变化趋势', component: YearlyCount },
  { name: '歌手地区分布', component: ArtistAttribution },
  { name: '用户分布热力图',component: UserAttribution },
  { name: '聊天智能体', component: LLM }
]

const current = ref(DecadePreference)
</script>
