<template>
  <q-dialog v-model="isOpen" persistent>
    <q-card style="min-width: 400px; max-width: 90vw;">
      
      <!-- Заголовок -->
      <q-card-section>
        <div class="text-h6">
          Churn новой возможности
        </div>
      </q-card-section>

      <q-separator />

      <!-- Форма -->
      <q-card-section class="q-gutter-md">
        <q-input
          v-model="form.screen"
          label="Экран"
          dense
          outlined
        />
        <q-input
          v-model="form.feature"
          label="Возможность"
          dense
          outlined
        />
        <q-input
          v-model="form.action"
          label="Действие"
          dense
          outlined
        />
      </q-card-section>

      <q-separator />

      <!-- Кнопки -->
      <q-card-actions align="right">
        <q-btn
          flat
          label="Отмена"
          color="grey"
          v-close-popup
        />
        <q-btn
          unelevated
          color="primary"
          label="Измерить"
          @click="onMeasure"
        />
      </q-card-actions>

    </q-card>
  </q-dialog>
</template>

<script setup lang="ts">
import { ref, reactive, defineExpose } from 'vue'
import { api } from 'boot/axios';
import { useQuasar } from 'quasar'

const $q = useQuasar()
const isOpen = ref(false)
// const nodeId = ref(null)

const form = reactive({
  node_id: '',
  screen: '',
  feature: '',
  action: ''
})

function open(id: string) {
  form.node_id = id
  form.screen = 'Новая заявка'
  form.feature = 'Выбор квартиры'
  form.action = 'Тап на квартиру'
  isOpen.value = true
}

// function close() {
//   isOpen.value = false
// }

async function onMeasure() {
  // тут ты дальше делаешь API / emit / расчёт
  // console.log('Payload:', { ...form })
  await api.post(`/api/predict`, form)
    .then(response => {
      console.log(response.data)
      const churnPercent = (response.data.churn_rate * 100).toFixed(2)
      const vsMeanPercent = response.data.churn_vs_mean_percent.toFixed(1)

      $q.notify({
        type: 'info',
        color: 'primary',
        position: 'top',
        timeout: 10000,
        message: `
          <div>
            <div><b>Отток:</b> ${churnPercent}%</div>
            <div><b>От среднего:</b> ${vsMeanPercent}%</div>
          </div>
        `,
        html: true
      })
    })

  // close()
}

defineExpose({
  open
})
</script>
