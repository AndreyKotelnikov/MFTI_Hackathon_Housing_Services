<script setup lang="ts">
import type { ResearchListItem } from 'src/types/api/Research';

interface ResearchItemProps {
  disabled: boolean
  research: ResearchListItem
}

withDefaults(defineProps<ResearchItemProps>(), {
  disabled: false,
})

</script>
<template>
  <q-card class="research-item q-pa-lg flex items-start">
    <div >
      <h6 class="text-secondary text-weight-semibold q-mt-xs q-mb-sm">
        <router-link :to="{ name: 'research', params: { id: research.id } }">
          {{ research.title }}
        </router-link>
      </h6>
        <div class="flex">
          <div class="research-item__desc research-item__desc_date text-weight-medium">
            <q-icon name="today"
                    color="primary">
            </q-icon>
            {{ research.created_at }}
          </div>
        </div>

    </div>
  </q-card>
</template>
