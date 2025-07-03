<template>
  <div
    class="flex flex-col max-w-3xl h-[95vh] max-h-[95vh] mx-auto bg-gray-900 rounded shadow-lg p-4 overflow-hidden"
  >
    <!-- 标题 -->
    <h2 class="text-2xl font-bold text-green-400 mb-4 text-center flex-shrink-0">
      智能聊天助手
    </h2>

    <!-- 聊天记录区域 -->
    <div
      ref="chatContainer"
      class="flex-grow overflow-y-auto mb-4 space-y-4 px-4 py-2 bg-gray-800 rounded"
    >
      <div
        v-for="(msg, idx) in messages"
        :key="idx"
        class="flex"
        :class="msg.role === 'user' ? 'justify-end' : 'justify-start'"
      >
        <div
          :class="[
            'max-w-[70%] px-4 py-2 rounded-lg whitespace-pre-wrap break-words',
            msg.role === 'user' ? 'bg-green-600 text-white' : 'bg-gray-700 text-gray-200',
          ]"
        >
          {{ msg.content }}
        </div>
      </div>
    </div>

    <!-- 输入框和发送按钮 -->
    <form @submit.prevent="sendMessage" class="flex gap-2 flex-shrink-0">
      <input
        v-model="input"
        type="text"
        placeholder="请输入您的问题，按回车发送"
        class="flex-1 rounded px-4 py-2 text-black focus:outline-none focus:ring-2 focus:ring-green-400"
        :disabled="loading"
        autocomplete="off"
        required
      />
      <button
        type="submit"
        :disabled="loading"
        class="bg-green-500 text-white px-6 rounded hover:bg-green-600 disabled:opacity-50 disabled:cursor-not-allowed transition"
      >
        {{ loading ? '发送中...' : '发送' }}
      </button>
    </form>
  </div>
</template>

<script setup>
import { ref, nextTick } from "vue";
import axios from "axios";

const messages = ref([]); // 聊天消息列表
const input = ref(""); // 输入框内容
const loading = ref(false); // 发送请求时禁用输入

const chatContainer = ref(null);

// 发送消息并调用后端接口
async function sendMessage() {
  if (!input.value.trim()) return;

  // 添加用户消息
  messages.value.push({ role: "user", content: input.value });
  loading.value = true;

  // 自动滚动到底部
  await nextTick();
  scrollToBottom();

  try {
    const res = await axios.post("/api/chat", { message: input.value });
    const reply = res.data.response || "抱歉，未收到回复。";

    messages.value.push({ role: "assistant", content: reply });
  } catch (error) {
    messages.value.push({ role: "assistant", content: "请求失败，请稍后重试。" });
  }

  input.value = "";
  loading.value = false;

  await nextTick();
  scrollToBottom();
}

// 滚动聊天窗口到底部
function scrollToBottom() {
  if (chatContainer.value) {
    chatContainer.value.scrollTop = chatContainer.value.scrollHeight;
  }
}
</script>

<style scoped>
/* 防止长文本撑破气泡 */
.break-words {
  word-break: break-word;
}

/* 去掉聊天区原先min/max-height限制，由flex-grow控制高度 */
</style>
