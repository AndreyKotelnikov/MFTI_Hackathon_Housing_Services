<script setup lang="ts">
import { useRouter } from 'vue-router'
const emit = defineEmits(['save', 'add', 'delete', 'fullscreen', 'download', 'zoom-in', 'zoom-out'])
const router = useRouter()
</script>

<template>
  <div class="graph-toolbar row items-center">
    <q-btn flat round dense icon="arrow_back" @click="router.push('/')" class="toolbar-btn" />
    <q-btn flat round dense icon="save" @click="emit('save')" class="toolbar-btn" />
    <q-btn flat round dense icon="add" @click="emit('add')" class="toolbar-btn" />
    <q-btn flat round dense icon="delete" @click="emit('delete')" class="toolbar-btn" />
    <q-btn flat round dense icon="fullscreen" @click="emit('fullscreen')" class="toolbar-btn" />
    <q-btn flat round dense icon="download" @click="emit('download')" class="toolbar-btn" />
    <q-btn flat round dense icon="zoom_in" @click="emit('zoom-in')" class="toolbar-btn" />
    <q-btn flat round dense icon="zoom_out" @click="emit('zoom-out')" class="toolbar-btn" />
  </div>
</template>

<style scoped lang="scss">
.graph-toolbar {
  min-width: 172px;
  height: 56px;
  background-color: #3461FF0D;
  border-radius: 10px;
  padding: 6px 10px;
  gap: 6px;
  z-index: 1;
}

.toolbar-btn {
  color: var(--q-dark-page);
  transition: 0.15s ease;

  &:hover {
    background: rgba(52, 97, 255, 0.15);
    color: var(--q-primary);
  }

  &:active {
    background: rgba(52, 97, 255, 0.25);
    color: var(--q-primary);
  }
}
</style>

