<script setup lang="ts">
import ResearchItem from 'components/researchComponents/ResearchItem.vue';
import { ref, onMounted } from 'vue';
// import { api } from 'boot/axios';
import type { ResearchListItem } from 'src/types/api/Research';

const isLoading = ref(true);
const isProcessing = ref(false);
const currentPage = ref<number>(1)
const totalPagesCount = ref<number>(1)

const researches = ref<ResearchListItem[]>([
  {
    id: 'graph_10к',
    title: "Визуализация на 10к",
    created_at: '12.12.2025',
  },
  {
    id: 'graph_100к',
    title: "Визуализация на 100к",
    created_at: '12.12.2025',
  },
  {
    id: 'graph_1000к',
    title: "Визуализация на 1кк",
    created_at: '12.12.2025',
  },
])

onMounted(() => {
  isLoading.value = false
})

</script>
<template>
  <q-page class="content-area flex column q-mx-auto">
    <div class="relative-position flex column col-grow">
      <q-linear-progress v-if="isLoading || isProcessing" indeterminate color="primary" />

      <div class="research-items q-mt-md" :class="{ 'loading-blur': isLoading }">
        <ResearchItem
          v-for="(research, index) in researches"
          :key="index"
          :research="research"
          :disabled="isProcessing"
        ></ResearchItem>
      </div>
        <q-pagination v-model="currentPage"
                      :max="totalPagesCount"
                      size="md"
                      direction-links
                      boundary-links />
    </div>
  </q-page>
</template>
