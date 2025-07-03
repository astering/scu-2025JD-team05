<template>
  <div class="min-h-screen flex items-center justify-center bg-slate-900 text-white">
    <div class="bg-slate-800 p-8 rounded-lg shadow-xl w-full max-w-sm">
      <h2 class="text-2xl font-bold mb-4 text-center">用户登录</h2>
      <input
        v-model="username"
        type="text"
        placeholder="用户名"
        class="input"
        autocomplete="username"
      />
      <input
        v-model="password"
        type="password"
        placeholder="密码"
        class="input"
        autocomplete="current-password"
      />
      <button @click="login" class="btn">登录</button>
    </div>
  </div>
</template>

<script setup>
import { ref } from 'vue'
import axios from 'axios'
import { useRouter } from 'vue-router'

const username = ref('')
const password = ref('')
const router = useRouter()

async function login() {
  try {
    const response = await axios.post('http://127.0.0.1:8000/api/auth/login', {
      username: username.value,
      password: password.value
    })
    const user = response.data
    localStorage.setItem('user', JSON.stringify(user))

    // 登录成功后，派发自定义事件通知
    window.dispatchEvent(new CustomEvent('user-logged-in', { detail: user }))

    await router.push('/home')
  } catch (err) {
    alert('登录失败：用户名或密码错误')
    console.error(err)
  }
}
</script>

<style scoped>
.input {
  width: 100%;
  padding: 10px;
  margin-bottom: 1rem;
  background: #1e293b;
  border: none;
  border-radius: 6px;
  color: white;
  font-size: 1rem;
  outline: none;
}
.input:focus {
  box-shadow: 0 0 0 2px #7c3aed;
}
.btn {
  width: 100%;
  background-color: #7c3aed;
  color: white;
  padding: 10px;
  border: none;
  border-radius: 6px;
  font-weight: 600;
  cursor: pointer;
  transition: background-color 0.3s ease;
}
.btn:hover {
  background-color: #6b21a8;
}
</style>
