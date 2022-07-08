export interface Equipment {
  attributes?: (AttributesEntityOrRelationsEntity)[] | null;
  type: string;
  tagNumber: string;
  description: string;
  relations?: (AttributesEntityOrRelationsEntity)[] | null;
  facility: string;
  area: string;
  system: string;
  discipline: string;
  procurementPackage: string;
  processLine: string;
  id: string;
  topTag: string;
}
export interface AttributesEntityOrRelationsEntity {
  id: string;
}
