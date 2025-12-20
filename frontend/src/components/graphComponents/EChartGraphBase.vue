<template>
  <div ref="chartRef" class="chart-container"></div>
</template>

<script setup lang="ts">
import { ref, onMounted, onBeforeUnmount } from 'vue'
import * as echarts from 'echarts'
// import type { Graph, GraphLink, GraphNode } from 'src/types/api/Research';
import type { Graph, GraphNode } from 'src/types/api/Research';
import type { ECharts, ECElementEvent } from 'echarts'


const props = defineProps({
  researchGraph: {
    type: Object as () => Graph,
    required: true
  },
  researchId: {
    type: [Number,String],
    required: true
  }
})


const $emit = defineEmits(['dragend', 'node-click'])

// const getNodeLinks = (nodeId: string, links: GraphLink[]): GraphLink[] => {
//   return links.filter((l: GraphLink) => l.source === nodeId || l.target === nodeId)
// }

const chartRef = ref<HTMLElement | null>(null)
let chartInstance: ECharts | null = null
// const isLoading = ref(false)
const isLoadingPage = ref(true)

const categories = [
  { name: 'fio', itemStyle: { color: '#3b82f6' } },
  { name: 'property', itemStyle: { color: '#10b981' } }
]

let currentZoom = 0.01
const localGraph = ref<Graph|null>()
const selectedNodes = ref<GraphNode[]>([])

const loadData = () => {

  if (!props.researchGraph) {
    alert('Не удалось загрузить данные исследования')
    return
  }

  // const links = props.researchGraph.links

  localGraph.value = props.researchGraph

  chartInstance?.setOption({
    series: [{
      data: localGraph.value.nodes,
      links: localGraph.value.links
    }]
  })
}

// Инициализация графа
const initChart = () => {
  if (!chartRef.value) return

  chartInstance = echarts.init(chartRef.value)

  const option = {
    // tooltip: {
    //   formatter: (params: EchartsParams) => params.data.category == 'fio' ? tooltipFioFormatter(params.data) : params.data.name,
    // },
    series: [{
      type: 'graph',
      layout: 'force',
      // layout: 'none',
      draggable: false,
      data: [],
      links: [],
      categories: categories,
      roam: true, // уже есть
      zoom: currentZoom,  // стартовый зум уменьшаем
      scaleLimit: {
        min: 0.002,  // позволяем очень далеко отдаляться
        max: 5
      },
      focusNodeAdjacency: true,
      symbolSize: 20,
      label: {
        show: true,
        position: 'right',
        // formatter: (params: EchartsParams) => labelFioFormatter(params.data),
        rich: {
          name: {
            fontSize: 12,
            color: '#333',
            align: 'center',
          },
          badge: {
            backgroundColor: '#409EFF',
            borderRadius: 8,
            padding: [4, 6, 2, 6],
            color: '#fff',
            fontSize: 10,
            align: 'center',
            verticalAlign: 'middle'
          }
        }
      },
      
      // Силы для звездообразной структуры
      force: {
        repulsion: 170,          // Увеличиваем отталкивание
        gravity: 0.01,            // Слабое притяжение к центру
        // edgeLength: 50,         // Фиксированная длина связей
        layoutAnimation: true,   // Анимация притяжения
      },
      
      // Иерархическое расположение
      circular: {
        rotateAngle: 0,          // Отключить круговое расположение
      },
      
      // Стиль связей
      lineStyle: {
        width: 1.5,
        curveness: 0,           // Прямые линии
        opacity: 0.9,
        color: 'source'
      },
      
      // Эффекты при наведении
      emphasis: {
        focus: 'adjacency',
        label: {
          show: true,
          fontWeight: 'bold'
        },
        lineStyle: {
          width: 2.5
        }
      }
    }]
  }
  chartInstance.setOption(option)
}

// Обработка ресайза
const handleResize = () => {
  chartInstance?.resize()
}

const clickNodeHandler = (params: ECElementEvent) => {
  const node = params.data as GraphNode
  console.log(node)
  $emit('node-click', node.id)
}

const zoomIn = () => {
  currentZoom = currentZoom * 1.2
  chartInstance?.setOption({
    series: [{ zoom: currentZoom }]
  })
}

const zoomOut = () => {
  currentZoom = currentZoom * 0.8
  chartInstance?.setOption({
    series: [{ zoom: currentZoom }]
  })
}

onMounted(() => {
  isLoadingPage.value = true
  initChart()
  loadData()
  isLoadingPage.value = false

  chartInstance?.on('click', (params: ECElementEvent) => {
    const node = params.data as GraphNode
    selectedNodes.value = [node]
    if (params.event?.target?.type != 'tspan') {
      void clickNodeHandler(params)
    }
  })

  window.addEventListener('resize', handleResize)
})

onBeforeUnmount(() => {
  window.removeEventListener('resize', handleResize)
  chartInstance?.dispose()
})

defineExpose({zoomIn, zoomOut})
</script>

<style>
.chart-container {
  width: 100%;
  flex: 1;
}
</style>