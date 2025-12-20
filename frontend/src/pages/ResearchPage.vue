<script setup lang="ts">
import { ref, watch, onMounted } from 'vue';
import type { ResearchDetail } from 'src/types/api/Research';
import { useRoute } from 'vue-router'
import EChartGraphBase from 'components/graphComponents/EChartGraphBase.vue'
import GraphToolbar from 'src/views/ResearchPage/GraphToolbar.vue'
import ChurnMeasureDialog from 'src/components/dialogs/ChurnMeasureDialog.vue'
import { api } from 'boot/axios';

const route = useRoute()

const researchId =  ref(String(route.params.id))
const isLoading = ref(true);

const researchData = ref<ResearchDetail|null>(null)
const chartGraphRef = ref<typeof EChartGraphBase>()
const churnDialog = ref<typeof ChurnMeasureDialog|null>(null)

const loadingData = async () => {
  isLoading.value = true
  researchData.value = null

  // await api.get('/data/graph_test.json')
  await api.get(`/data/${researchId.value}.json`)
    .then(response => {
      researchData.value = {
        id: 1,
        title: 'Ololo',
        graph: response.data,
        created_at: '12.12.2025'
      }
      console.log(response.data)
      isLoading.value = false
    })
}

watch(() => route.params.id, () => researchId.value = String(route.params.id))
watch(() => researchId.value, () => void loadingData())

const addNodeModal = (nodeId: string) => {
  // console.log(churnDialog.value)
  churnDialog.value?.open(nodeId)
}

onMounted(() => {
  void loadingData()
})

</script>
<template>
  <q-page class="content-area flex column q-mx-auto">
    <div class="relative-position flex column col-grow">

      <q-linear-progress v-if="isLoading" indeterminate absolute color="primary" />

      <GraphToolbar class="absolute"
        @zoom-in="chartGraphRef?.zoomIn()"
        @zoom-out="chartGraphRef?.zoomOut()"
      />

      <div class="research-graph" :class="{ 'loading-blur': isLoading }">
        <EChartGraphBase v-if="researchData"
          ref="chartGraphRef"
          :researchGraph="researchData.graph"
          :researchId="researchId"
          @dragend="researchData = $event"
          @node-click="addNodeModal"
        />
      </div>
    </div>
  </q-page>
  <ChurnMeasureDialog ref="churnDialog" />
</template>
<style lang="scss" scoped>

.research-graph {
  display: flex;
  flex-direction: column;
  flex-grow: 1;
  flex: 1;
  overflow: hidden;
}

.loading-blur {
  opacity: 0.5;
  pointer-events: none;
  filter: grayscale(40%);
}

</style>
