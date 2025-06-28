import { createRouter, createWebHistory } from 'vue-router'
import Login from '@/components/login/Login.vue'
import DashboardPage from '@/components/dashboard/Dashboard.vue'
import Home from "@/components/main/Home.vue";
import RankPage from '@/components/rank/Rank.vue'
import PlaylistPage from '@/components/rank/Playlist.vue'
import ArtistPage from '@/components/rank/Artist.vue'
import Newest from '@/components/rank/Newest.vue'

const routes = [
  { path: '/', redirect: '/login' },
  { path: '/login', component: Login },
  { path: '/home', component: Home },
  { path: '/dashboard', component: DashboardPage },
  { path: '/rank', component: RankPage },
  { path: '/playlist', component: PlaylistPage },
  { path: '/artists', component: ArtistPage},
  { path:'/newest', component: Newest}
]

const router = createRouter({
  history: createWebHistory(),
  routes,
})

// 全局路由守卫：未登录时强制跳转登录页
router.beforeEach((to, from, next) => {
  const user = localStorage.getItem("user");
  if (!user && to.path !== "/login") {
    next("/login");
  } else {
    next();
  }
});

export default router;

