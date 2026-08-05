export type Locale = "en" | "pt";
export type WorkState = "todo" | "in_progress" | "blocked" | "done" | "failed";

export interface LocalizedText {
  en?: string;
  pt?: string;
}
export interface PresentationField {
  label: LocalizedText;
  value: string;
}
export interface SourcePresentation {
  kind: string;
  title: string;
  subtitle?: string;
  originalUrl?: string;
  evidence?: PresentationField[];
}
export interface WorkPresentation {
  workflowTitle: string;
  personaName: string;
  personaAvatar?: string;
  locale?: Locale;
}
export interface BusinessStep {
  key: string;
  label: LocalizedText;
  state: string;
  startedAt?: string;
}
export interface BlockerSummary {
  id: string;
  kind: string;
  title: LocalizedText;
}
export interface WorkItem {
  id: string;
  workflowKey: string;
  title: string;
  source: SourcePresentation;
  presentation: WorkPresentation;
  currentStep?: BusinessStep;
  blocker?: BlockerSummary;
  currentAttemptId: string;
  state: WorkState;
  createdAt: string;
  updatedAt: string;
  startedAt?: string;
  finishedAt?: string;
  valueConfigured: boolean;
  valueModel?: ValueModel;
  estimatedValue?: string;
  valueCurrency?: string;
  valueDisputed: boolean;
  failureSummary?: unknown;
}
export interface ValueModel {
  ref?: string;
  version?: string;
  formula?: string;
  currency?: string;
  baselineSeconds?: number;
  loadedHourlyCost?: string;
  assumptions?: unknown;
  explanation?: LocalizedText | string;
}
export interface DashboardSummary {
  counts: Record<WorkState, number>;
  value: {
    configured: boolean;
    estimated: boolean;
    totals: CurrencyAmount[];
    models: ValueModel[];
  };
  trend: Array<{ date: string; amount: string; currency: string }>;
}
export interface CurrencyAmount {
  amount: string;
  currency: string;
}
export interface Attempt {
  id: string;
  workItemId: string;
  number: number;
  definitionKey: string;
  definitionVersion: string;
  revisedFromAttemptId?: string;
  actionableGuidance?: string;
  createdAt: string;
  finishedAt?: string;
}
export interface NodeExecution {
  id: string;
  attemptId: string;
  nodeKey: string;
  businessLabel: LocalizedText;
  visit: number;
  executionSeq: number;
  kind: string;
  state: string;
  outcome?: string;
  summary?: string;
  output?: unknown;
  artifactRefs?: Array<{
    artifactId: string;
    role?: string;
    metadata?: unknown;
  }>;
  startedAt?: string;
  finishedAt?: string;
  createdAt: string;
}
export interface TimelineEvent {
  cursor: number;
  id: string;
  workItemId: string;
  attemptId?: string;
  nodeExecutionId?: string;
  code: string;
  params: Record<string, unknown>;
  artifactRefs?: Array<{ artifactId: string }>;
  createdAt: string;
}
export interface Blocker {
  id: string;
  workItemId: string;
  attemptId: string;
  nodeExecutionId?: string;
  kind: string;
  title: LocalizedText;
  description: LocalizedText;
  responseSchema: JSONSchema;
  allowedOutcomes: string[];
  requiredConsequences?: Consequence[];
  state: string;
}
export interface JSONSchema {
  type?: string;
  title?: string;
  required?: string[];
  properties?: Record<string, JSONSchema>;
  enum?: unknown[];
  items?: JSONSchema;
  minLength?: number;
}
export interface Consequence {
  invocationId: string;
  summary: LocalizedText;
  state: string;
}
export interface Feedback {
  id: string;
  workItemId: string;
  attemptId: string;
  category: string;
  explanation?: string;
  correctedResult?: unknown;
  createdAt: string;
  revisedAttemptId?: string;
}
export interface WorkArtifact {
  artifactId: string;
  attemptId: string;
  nodeExecutionId?: string;
  role: string;
  metadata: Record<string, unknown>;
  mimeType: string;
  sizeBytes?: number;
  digest?: string;
  createdAt: string;
}
export interface DetailData {
  item: WorkItem;
  attempts: Attempt[];
  nodes: NodeExecution[];
  timeline: TimelineEvent[];
  blocker: Blocker | null;
  feedback: Feedback[];
  consequences: Consequence[];
  artifacts: WorkArtifact[];
}
