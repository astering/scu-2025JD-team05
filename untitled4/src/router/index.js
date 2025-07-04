import { createRouter, createWebHistory } from 'vue-router'

// ҳ���������
import Login from '@/components/login/Login.vue'
import DashboardPage from '@/components/dashboard/Dashboard.vue'
import Home from "@/components/main/Home.vue"
import RankPage from '@/components/rank/Rank.vue'
import ArtistPage from '@/components/rank/Artist.vue'
import SearchPage from '@/components/main/Search.vue'

const routes = [
  { path: '/', redirect: '/login' },
  { path: '/login', component: Login },
  { path: '/home', component: Home },
  { path: '/dashboard', component: DashboardPage },
  { path: '/rank', component: RankPage },
  { path: '/artists', component: ArtistPage },
  { path: '/search', name: 'search', component: SearchPage }
]

const router = createRouter({
  history: createWebHistory(),
  routes,
})

// ȫ��·��������δ��¼ʱǿ����ת��¼ҳ
router.beforeEach((to, from, next) => {
  const user = localStorage.getItem("user");
  if (!user && to.path !== "/login") {
    next("/login");
  } else {
    next();
  }
});

export default router;
