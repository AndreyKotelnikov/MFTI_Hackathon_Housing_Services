export interface ResearchListItem {
  id: number|string|null;
  title: string;
  created_at: string;
}

export interface ResearchDetail {
  id: number|string;
  title: string;
  graph: Graph;
  created_at: string;
}

export interface GraphLink {
  source: string;
  target: string;
}

export interface GraphNode {
  id: string;
  name: string;
  category: string;
  // symbolSize?: number;
  // itemStyle?: {
  //   color?: string;
  // }
  // fixed: boolean
  x?: number
  y?: number
}

export interface Graph {
  links: GraphLink[]
  nodes: GraphNode[]
}
