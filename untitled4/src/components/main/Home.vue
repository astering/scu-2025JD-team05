<<template>
  <div class="min-h-screen flex flex-col bg-gradient-to-br from-slate-900 to-slate-800 text-white">
    <Header @menuChanged="onMenuChange">
      <template #search>
        <!-- 插槽中嵌入 SearchBar -->
        <SearchBar />
      </template>
    </Header>
    <MainContent />
  </div>
</template>

<script setup>
import { useRouter } from 'vue-router'
import Header from '@/components/main/Header.vue'
import MainContent from '@/components/main/MainContent.vue'
import SearchBar from "@/components/main/SearchBar.vue";

const router = useRouter()

function onMenuChange(index) {
  console.log("菜单切换到索引:", index)
}

</script>
