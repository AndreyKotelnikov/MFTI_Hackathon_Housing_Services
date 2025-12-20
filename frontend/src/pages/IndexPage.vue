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
    id: 'graph_full',
    title: "Полная модель переходов пользователей",
    created_at: '18.12.2025',
  },
  {
    id: 'graph_main',
    title: "Популярные разделы",
    created_at: '20.12.2025',
  },
  {
    id: 'graph_main',
    title: "Точки наибольшей утечки",
    created_at: '20.12.2025',
  },
])

onMounted(() => {
  isLoading.value = false
})

</script>
<template>
  <q-page class="content-area q-pa-md flex justify-center">
    <div class="page-wrapper full-width">

      <!-- Лоадер -->
      <q-linear-progress
        v-if="isLoading || isProcessing"
        indeterminate
        color="primary"
        class="q-mb-md"
      />

      <!-- Контент -->
      <q-card flat bordered class="q-pa-md">

        <q-card-section class="q-pb-md">
          <div class="text-h4 text-weight-medium">
            Прогноз оттока клиентов
          </div>
          <div class="text-subtitle2 text-grey-7">
            Графовая модель
          </div>
        </q-card-section>

        <q-card-section class="q-pa-none">
          <div
            class="research-items"
            :class="{ 'loading-blur': isLoading }"
          >
            <ResearchItem
              v-for="(research, index) in researches"
              :key="index"
              :research="research"
              :disabled="isProcessing"
              class="q-mb-sm"
            />
          </div>
        </q-card-section>

        <!-- Пагинация -->
        <q-separator class="q-my-md" />

        <q-card-actions align="center">
          <q-pagination
            v-model="currentPage"
            :max="totalPagesCount"
            direction-links
            boundary-links
            size="md"
          />
        </q-card-actions>

      </q-card>
    </div>
  </q-page>
</template>

<style lang="scss">
.page-wrapper {
  max-width: 900px;
}

.loading-blur {
  filter: blur(2px);
  pointer-events: none;
  opacity: 0.6;
}
</style>