<script setup lang="ts">
import { ref, watch, onMounted } from 'vue';
import type { ResearchDetail } from 'src/types/api/Research';
import { useRoute } from 'vue-router'
import EChartGraphBase from 'components/graphComponents/EChartGraphBase.vue'
import GraphToolbar from 'src/views/ResearchPage/GraphToolbar.vue'
import { api } from 'boot/axios';

const route = useRoute()

const researchId =  ref(String(route.params.id))
const isLoading = ref(true);

const researchData = ref<ResearchDetail|null>(null)
const chartGraphRef = ref<typeof EChartGraphBase>()

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

//   setTimeout(() => {
//     researchData.value = {
//       id: researchId.value,
//       title: 'Ololo',
//       created_at: '12.12.2025',
//       graph: {
//   "nodes": [
//     {
//       "id": "a92d6488064da2f1c55adaa8391a4d122f75f0cb5d5e3af74d0e35724094c3c2",
//       "name": "Еще",
//       "category": "screen",
//       "x": 0,
//       "y": 0
//     },
//     {
//       "id": "053df6c6de4b636fe55a8da1873a5fc645a1051b74b0779fbfc8687908ad8ecd",
//       "name": "Новая заявка",
//       "category": "screen",
//       "x": 0,
//       "y": 0
//     },
//     {
//       "id": "509aa1a337054a47b8d1a4bf6b1d868406655f80b0b19c58be8920d8456d6441",
//       "name": "Важное",
//       "category": "screen",
//       "x": 0,
//       "y": 0
//     },
//     {
//       "id": "2b9e33f273a05d5c5f9074a1459e0d374bc80b4ff15ab5396b0ea3bcf014856e",
//       "name": "Заявки",
//       "category": "screen",
//       "x": 0,
//       "y": 0
//     },
//     {
//       "id": "2da6995836bb6c95663b9b2c1b2aac77709b52666226f7d62215feac55680ff5",
//       "name": "Объявления",
//       "category": "screen",
//       "x": 0,
//       "y": 0
//     },
//     {
//       "id": "2a3c4c73d2076c3b58d563079b1e57f2daa4012b2a2ec7b3a0b77560356aff47",
//       "name": "Связаться с владельцем ТС",
//       "category": "screen",
//       "x": 0,
//       "y": 0
//     },
//     {
//       "id": "d4c8d7010435c104d08426a2b601ffd4fcb440f6ac81f225dd9d59d59cd8d620",
//       "name": "Переход в раздел 'Заявки'",
//       "category": "function",
//       "x": 0,
//       "y": 0
//     },
//     {
//       "id": "723d254d762472a8eac8ddebc287ce5cd03c266130f5be7afd1b01f72cc925b2",
//       "name": "Выбор квартиры",
//       "category": "function",
//       "x": 0,
//       "y": 0
//     },
//     {
//       "id": "3400472d567f094f3c574618c6e933a2ce6bda13ec7cc8b3dc743f36a0c291d4",
//       "name": "Переход в раздел 'Услуги'",
//       "category": "function",
//       "x": 0,
//       "y": 0
//     },
//     {
//       "id": "546859d5bd463e2630067c1ca26818d3041047c23a2277e47ff2d634c9caf2ca",
//       "name": "Просмотр уведомления",
//       "category": "function",
//       "x": 0,
//       "y": 0
//     },
//     {
//       "id": "c1b0b87ef77176044ef4a261ccfdfd38dfece6a23d64546b6e39b7a8e9e79c07",
//       "name": "Вызов поиска",
//       "category": "function",
//       "x": 0,
//       "y": 0
//     },
//     {
//       "id": "8fcd3f4d3f6720ba5fb80b458916510e493fa01c6b45a57cb7bb739aca910a16",
//       "name": "Отправка заявки",
//       "category": "function",
//       "x": 0,
//       "y": 0
//     },
//     {
//       "id": "e66e94af697a85fbd9f2a80241b2c42890d94c2d26470443d92cc95d3a68d9a1",
//       "name": "Возврат на предыдущий этап",
//       "category": "function",
//       "x": 0,
//       "y": 0
//     },
//     {
//       "id": "fa652484da3ab6da39d9b310a94df134ece40bee60583358e5bed9d9e0bc65d6",
//       "name": "Выбор через теги",
//       "category": "function",
//       "x": 0,
//       "y": 0
//     },
//     {
//       "id": "6f9ebec140186958887fafb17a7a34e51c9aa450338e07691a32f138f7e7b2c6",
//       "name": "Выбор объекта заявки",
//       "category": "function",
//       "x": 0,
//       "y": 0
//     },
//     {
//       "id": "40ac7ff3fe1718281e5bacbdf010c43647989ef93cabb4bc8f918bf6e9c32709",
//       "name": "Выбор категории заявки",
//       "category": "function",
//       "x": 0,
//       "y": 0
//     },
//     {
//       "id": "a48042fbb6c794ed6e35edf00244b231221b874a5cba13d8267c10ab2852900d",
//       "name": "Выбор тематики заявки",
//       "category": "function",
//       "x": 0,
//       "y": 0
//     },
//     {
//       "id": "9ca1027db0f1ede44149edfe7343533e62c495c6f8c55e7902cfa5253e1daa32",
//       "name": "Переход в раздел 'Приборы учета'",
//       "category": "function",
//       "x": 0,
//       "y": 0
//     },
//     {
//       "id": "889748cd8176ffc38b431e92d28007426cdfef5e1b4a0e7124adb897b5e4503b",
//       "name": "Переход в раздел 'Мой транспорт'",
//       "category": "function",
//       "x": 0,
//       "y": 0
//     },
//     {
//       "id": "744824e5107e9aec0d56617dadf23448be39625dcd7ed97ceeb78cc1956feebe",
//       "name": "Переход в раздел 'Профиль'",
//       "category": "function",
//       "x": 0,
//       "y": 0
//     },
//     {
//       "id": "954ef2e72588b993b7ba8c951b89336bf2a5b711f57404612c893bf3a595acc1",
//       "name": "Переход в раздел 'Опросы и собрания собственников'",
//       "category": "function",
//       "x": 0,
//       "y": 0
//     },
//     {
//       "id": "10ab135e2b841282faa5ce63ad7a99d4ecbe54820fd813f76d9213dc3629f383",
//       "name": "Выбор темы через теги",
//       "category": "function",
//       "x": 0,
//       "y": 0
//     },
//     {
//       "id": "49473014a89745b9e368d79252ecc7b2c2cf0715ca4db22914b6ff10f9e7806b",
//       "name": "Выбор актуальной темы",
//       "category": "function",
//       "x": 0,
//       "y": 0
//     },
//     {
//       "id": "f5fe5a671cbc7006b6d980f0e59efa7e22f366c7111622d977f3998addb22b18",
//       "name": "Выбор темы в поиске",
//       "category": "function",
//       "x": 0,
//       "y": 0
//     },
//     {
//       "id": "91a6b8843dec38b089675d6b4296625ae1d87e90809ca8d9681f06ff4739e16f",
//       "name": "Переход в раздел 'Электронный консьерж'",
//       "category": "function",
//       "x": 0,
//       "y": 0
//     },
//     {
//       "id": "9fcb0c9925383cc0e67a5f2357b66fad4e704e134ef17a97104ca72c118c73b1",
//       "name": "Переход в раздел 'Мои платежи'",
//       "category": "function",
//       "x": 0,
//       "y": 0
//     },
//     {
//       "id": "a5d3c94ccaafd0e576570ce6573a0765079bd507a65586d3de5a788314d3cca4",
//       "name": "Переход к просмотру заявки",
//       "category": "function",
//       "x": 0,
//       "y": 0
//     },
//     {
//       "id": "fe0a9a3b17a4be3a15d4784aa7ab43be36dab48f206b6562ba56429e1a49c2b6",
//       "name": "Раскрытие вкладки 'Соседей'",
//       "category": "function",
//       "x": 0,
//       "y": 0
//     },
//     {
//       "id": "266337ed97b10f0e2508048fa2c600d93e22ed1146006a9d90b418be0b8130a4",
//       "name": "Переход в раздел 'Объявления'",
//       "category": "function",
//       "x": 0,
//       "y": 0
//     },
//     {
//       "id": "974b8de02c413530949043cafc1e2a4a3c2fc337035bc39bf9bc51bb77ec1523",
//       "name": "Открытие экрана",
//       "category": "function",
//       "x": 0,
//       "y": 0
//     },
//     {
//       "id": "9db612c3ff0df98d53dec939560195a2578fd601607f17e5f3622607ee4a554c",
//       "name": "Раскрытие вкладки 'Мои'",
//       "category": "function",
//       "x": 0,
//       "y": 0
//     },
//     {
//       "id": "9d947d9ca2e6cb65964adba01427bbbe0198d311f9575e42774b3712cca1e691",
//       "name": "Переход в раздел 'База знаний'",
//       "category": "function",
//       "x": 0,
//       "y": 0
//     },
//     {
//       "id": "29dea78b9879180583f76a24858cee415bf18a959225832f4521eab221202b8e",
//       "name": "Просьба убрать ТС",
//       "category": "function",
//       "x": 0,
//       "y": 0
//     }
//   ],
//   "links": [
//     {
//       "source": "a92d6488064da2f1c55adaa8391a4d122f75f0cb5d5e3af74d0e35724094c3c2",
//       "target": "d4c8d7010435c104d08426a2b601ffd4fcb440f6ac81f225dd9d59d59cd8d620"
//     },
//     {
//       "source": "053df6c6de4b636fe55a8da1873a5fc645a1051b74b0779fbfc8687908ad8ecd",
//       "target": "723d254d762472a8eac8ddebc287ce5cd03c266130f5be7afd1b01f72cc925b2"
//     },
//     {
//       "source": "a92d6488064da2f1c55adaa8391a4d122f75f0cb5d5e3af74d0e35724094c3c2",
//       "target": "3400472d567f094f3c574618c6e933a2ce6bda13ec7cc8b3dc743f36a0c291d4"
//     },
//     {
//       "source": "509aa1a337054a47b8d1a4bf6b1d868406655f80b0b19c58be8920d8456d6441",
//       "target": "546859d5bd463e2630067c1ca26818d3041047c23a2277e47ff2d634c9caf2ca"
//     },
//     {
//       "source": "053df6c6de4b636fe55a8da1873a5fc645a1051b74b0779fbfc8687908ad8ecd",
//       "target": "c1b0b87ef77176044ef4a261ccfdfd38dfece6a23d64546b6e39b7a8e9e79c07"
//     },
//     {
//       "source": "053df6c6de4b636fe55a8da1873a5fc645a1051b74b0779fbfc8687908ad8ecd",
//       "target": "8fcd3f4d3f6720ba5fb80b458916510e493fa01c6b45a57cb7bb739aca910a16"
//     },
//     {
//       "source": "053df6c6de4b636fe55a8da1873a5fc645a1051b74b0779fbfc8687908ad8ecd",
//       "target": "e66e94af697a85fbd9f2a80241b2c42890d94c2d26470443d92cc95d3a68d9a1"
//     },
//     {
//       "source": "053df6c6de4b636fe55a8da1873a5fc645a1051b74b0779fbfc8687908ad8ecd",
//       "target": "fa652484da3ab6da39d9b310a94df134ece40bee60583358e5bed9d9e0bc65d6"
//     },
//     {
//       "source": "053df6c6de4b636fe55a8da1873a5fc645a1051b74b0779fbfc8687908ad8ecd",
//       "target": "6f9ebec140186958887fafb17a7a34e51c9aa450338e07691a32f138f7e7b2c6"
//     },
//     {
//       "source": "053df6c6de4b636fe55a8da1873a5fc645a1051b74b0779fbfc8687908ad8ecd",
//       "target": "40ac7ff3fe1718281e5bacbdf010c43647989ef93cabb4bc8f918bf6e9c32709"
//     },
//     {
//       "source": "053df6c6de4b636fe55a8da1873a5fc645a1051b74b0779fbfc8687908ad8ecd",
//       "target": "a48042fbb6c794ed6e35edf00244b231221b874a5cba13d8267c10ab2852900d"
//     },
//     {
//       "source": "a92d6488064da2f1c55adaa8391a4d122f75f0cb5d5e3af74d0e35724094c3c2",
//       "target": "9ca1027db0f1ede44149edfe7343533e62c495c6f8c55e7902cfa5253e1daa32"
//     },
//     {
//       "source": "a92d6488064da2f1c55adaa8391a4d122f75f0cb5d5e3af74d0e35724094c3c2",
//       "target": "889748cd8176ffc38b431e92d28007426cdfef5e1b4a0e7124adb897b5e4503b"
//     },
//     {
//       "source": "a92d6488064da2f1c55adaa8391a4d122f75f0cb5d5e3af74d0e35724094c3c2",
//       "target": "744824e5107e9aec0d56617dadf23448be39625dcd7ed97ceeb78cc1956feebe"
//     },
//     {
//       "source": "a92d6488064da2f1c55adaa8391a4d122f75f0cb5d5e3af74d0e35724094c3c2",
//       "target": "954ef2e72588b993b7ba8c951b89336bf2a5b711f57404612c893bf3a595acc1"
//     },
//     {
//       "source": "053df6c6de4b636fe55a8da1873a5fc645a1051b74b0779fbfc8687908ad8ecd",
//       "target": "10ab135e2b841282faa5ce63ad7a99d4ecbe54820fd813f76d9213dc3629f383"
//     },
//     {
//       "source": "053df6c6de4b636fe55a8da1873a5fc645a1051b74b0779fbfc8687908ad8ecd",
//       "target": "49473014a89745b9e368d79252ecc7b2c2cf0715ca4db22914b6ff10f9e7806b"
//     },
//     {
//       "source": "053df6c6de4b636fe55a8da1873a5fc645a1051b74b0779fbfc8687908ad8ecd",
//       "target": "f5fe5a671cbc7006b6d980f0e59efa7e22f366c7111622d977f3998addb22b18"
//     },
//     {
//       "source": "a92d6488064da2f1c55adaa8391a4d122f75f0cb5d5e3af74d0e35724094c3c2",
//       "target": "91a6b8843dec38b089675d6b4296625ae1d87e90809ca8d9681f06ff4739e16f"
//     },
//     {
//       "source": "a92d6488064da2f1c55adaa8391a4d122f75f0cb5d5e3af74d0e35724094c3c2",
//       "target": "9fcb0c9925383cc0e67a5f2357b66fad4e704e134ef17a97104ca72c118c73b1"
//     },
//     {
//       "source": "2b9e33f273a05d5c5f9074a1459e0d374bc80b4ff15ab5396b0ea3bcf014856e",
//       "target": "a5d3c94ccaafd0e576570ce6573a0765079bd507a65586d3de5a788314d3cca4"
//     },
//     {
//       "source": "2b9e33f273a05d5c5f9074a1459e0d374bc80b4ff15ab5396b0ea3bcf014856e",
//       "target": "fe0a9a3b17a4be3a15d4784aa7ab43be36dab48f206b6562ba56429e1a49c2b6"
//     },
//     {
//       "source": "a92d6488064da2f1c55adaa8391a4d122f75f0cb5d5e3af74d0e35724094c3c2",
//       "target": "266337ed97b10f0e2508048fa2c600d93e22ed1146006a9d90b418be0b8130a4"
//     },
//     {
//       "source": "2da6995836bb6c95663b9b2c1b2aac77709b52666226f7d62215feac55680ff5",
//       "target": "974b8de02c413530949043cafc1e2a4a3c2fc337035bc39bf9bc51bb77ec1523"
//     },
//     {
//       "source": "2b9e33f273a05d5c5f9074a1459e0d374bc80b4ff15ab5396b0ea3bcf014856e",
//       "target": "9db612c3ff0df98d53dec939560195a2578fd601607f17e5f3622607ee4a554c"
//     },
//     {
//       "source": "a92d6488064da2f1c55adaa8391a4d122f75f0cb5d5e3af74d0e35724094c3c2",
//       "target": "9d947d9ca2e6cb65964adba01427bbbe0198d311f9575e42774b3712cca1e691"
//     },
//     {
//       "source": "2a3c4c73d2076c3b58d563079b1e57f2daa4012b2a2ec7b3a0b77560356aff47",
//       "target": "29dea78b9879180583f76a24858cee415bf18a959225832f4521eab221202b8e"
//     }
//   ]
// }
//     }
//     isLoading.value = false
//   }, 1000);
}

watch(() => route.params.id, () => researchId.value = String(route.params.id))
watch(() => researchId.value, () => void loadingData())

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
        />
      </div>
    </div>
  </q-page>
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
