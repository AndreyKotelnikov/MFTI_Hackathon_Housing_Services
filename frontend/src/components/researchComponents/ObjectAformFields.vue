<script setup lang="ts">
import type { ResearchListItem } from 'src/types/api/Research';
import { ref } from 'vue';
import { QForm } from 'quasar'

interface ObjectFormProps {
  disabled?: boolean
  modelValue: ResearchListItem
}

const emit = defineEmits(['update:modelValue'])

const props = withDefaults(defineProps<ObjectFormProps>(), {
  disabled: false
})

const formRef = ref<typeof QForm>()

function updateField(field: string, value: unknown) {
  emit('update:modelValue', { ...props.modelValue, [field]: value })
}

const validateForm = async () => {
  let isSuccess = false
  await formRef.value?.validate().then((success:boolean) => {
    isSuccess = success
  })
  return isSuccess
}

defineExpose({
  validateForm
})

</script>
<template>
  <q-form ref="formRef">
    <q-card-section class="dialog-content flex q-px-lg justify-between">
      <div class="dialog-inputs">
        <div class="q-mb-md">
          <div class="q-mb-xs">
            <label for="secondname"
                   class="text-weight-medium">Название исследования</label>
          </div>
          <div>
            <q-input
              :model-value="modelValue.title"
              @update:model-value="v => updateField('title', v)"
              id="title"
              outlined
              dense
              placeholder="Введите название эксперемента"
            />
          </div>
        </div>

      </div>
    </q-card-section>
  </q-form>
</template>
