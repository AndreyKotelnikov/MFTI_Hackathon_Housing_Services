<script setup lang="ts">
interface IDialogHeaderProps {
  title: string
}

withDefaults(defineProps<IDialogHeaderProps>(), {
  title: ''
})

</script>
<template>
  <q-bar class="dialog-header q-px-lg">
    <div class="dialog-header__title text-secondary text-weight-semibold">
      {{ title }}
    </div>
    <q-space />
    <q-btn dense
           flat
           icon="close"
           v-close-popup>
      <q-tooltip>Закрыть</q-tooltip>
    </q-btn>
  </q-bar>
</template>
<style lang="scss">
.dialog-header {

  background-color: $blue-header;
  height: 48px;

  &__title {
    font-size: 16px;
  }
}
</style>
