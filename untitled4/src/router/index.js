import { createRouter, createWebHistory } from 'vue-router'
import Login from '@/components/login/Login.vue'
import DashboardPage from '@/components/dashboard/Dashboard.vue'
import Home from "@/components/main/Home.vue";

const routes = [
  { path: '/', redirect: '/login' },
  { path: '/login', component: Login },
  { path: '/home', component: Home },
  { path: '/dashboard', component: DashboardPage },
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

