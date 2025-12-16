<script setup lang="ts">
import { ref } from 'vue'
import DialogHeader from 'components/dialogs/DialogHeader.vue';
import ObjectAFormFields from 'components/researchComponents/ObjectAformFields.vue'
import type { ResearchListItem } from 'src/types/api/Research';

const isShowDialog = ref(false)
const isLoading = ref(true);
const researchRequest = ref<ResearchListItem | null>(null)
const dialogResolve = ref<(value: ResearchListItem) => void>()
const dialogReject = ref<(reason?: Error) => void>()
const objectFormFields = ref<typeof ObjectAFormFields>()

const validateForm = async () => {
  return await objectFormFields.value?.validateForm()
}

async function open() {

  researchRequest.value = null
  isShowDialog.value = true

  researchRequest.value = {
    id: null,
    title: "",
    created_at: ""
  }

  return new Promise((resolve, reject) => {
    dialogResolve.value = resolve
    dialogReject.value = reject
  })
}

function hide() {
  isShowDialog.value = false
}

async function resolveRequestForm() {
  if (researchRequest.value && dialogResolve.value) {
    if (await validateForm()) {
      dialogResolve.value(researchRequest.value)
      hide()
    }
  }
}

defineExpose({
  open
})

</script>
<template>
  <q-dialog v-model="isShowDialog">
    <q-card class="dialog">

      <DialogHeader title="Добавить объект"></DialogHeader>

      <q-linear-progress v-if="isLoading" indeterminate color="primary" />

      <ObjectAFormFields v-if="researchRequest"
        ref="objectFormFields"
        v-model="researchRequest"
      />

      <q-card-section class="flex justify-end q-pt-sm q-px-lg">
        <q-btn color="primary"
          no-caps
          class="text-weight-semibold q-py-sm q-px-md"
          icon="add"
          label="Добавить"
          :disable="isLoading"
          @click="resolveRequestForm"
        ></q-btn>
      </q-card-section>

    </q-card>
  </q-dialog>
</template>
<style lang="scss" scoped>
.dialog {
  border: 1px solid $border;
  width: 560px;

  &__content {
    gap: 24px;

    @media (min-width: 600px) {
      flex-wrap: nowrap;
      gap: 60px;
    }
  }

  &__research-types {
    gap: 10px;

    @media (min-width: 600px) {
      gap: 24px;
      flex-direction: column;
    }
  }

  &__inputs {
    flex-grow: 1;
  }

  &__bottom-inputs {
    display: grid;
    gap: 16px;
    grid-template-columns: 1fr;

    @media (min-width: 600px) {
      grid-template-columns: 1fr 1fr;
    }
  }
}
</style>
