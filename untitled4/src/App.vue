<template>
  <router-view />
</template>

<script setup>
// 这里不再处理登录状态，由路由守卫完成

</script>

<style>
body {
  background-color: #0f172a;
  color: #e0e7ff;
  font-family: "Helvetica Neue", Arial, sans-serif;
}
.card-glass {
  background: rgba(30, 41, 59, 0.6);
  backdrop-filter: blur(10px);
}
.player-btn {
  cursor: pointer;
  transition: all 0.3s ease;
}
.card-hover:hover {
  box-shadow: 0 0 15px rgba(139, 92, 246, 0.6);
}
</style>
