import {z} from 'zod';

const digest=z.string().regex(/^[a-f0-9]{64}$/);
const id=z.string().regex(/^[a-zA-Z0-9_-]{1,80}$/);
const artifact=z.string().regex(/^[a-zA-Z0-9][a-zA-Z0-9_.-]{0,100}$/).refine(name=>
  !['candidate.json','changes.patch','validation-intent.json','integration-readiness.json','task-instructions-history.json'].includes(name));
const requirements=z.array(id).min(1).max(30);
const requirementIdentity=z.object({id,digest}).strict();
export const readinessObligationSchema=z.discriminatedUnion('kind',[
  z.object({id,requirements,kind:z.literal('local'),name:artifact,field:z.string().min(1).max(100),
    equals:z.union([z.string().max(1000),z.number().finite(),z.boolean()])}).strict(),
  z.object({id,requirements,kind:z.literal('registered_ci'),
    file:z.string().max(300).regex(/^tests\/(?:[a-zA-Z0-9_-]+\/)*[a-zA-Z0-9_-]+(?:\.[a-zA-Z0-9_-]+)*\.test\.[cm]?[jt]s$/),
    name:z.string().min(1).max(500).refine(value=>!value.includes('\0'))}).strict(),
]);
/** An authenticated finite map, never a digest of an LLM's verification prose. */
export const candidateReadinessMapSchema=z.object({version:z.literal(1),enabled:z.boolean(),
  profile:z.literal('agentos.validate'),policyRegistrationId:z.uuid(),
  requirements:z.array(requirementIdentity).min(1).max(30),
  // No default: an absent scope preserves the original whole-objective bytes.
  scope:z.object({kind:z.literal('batch'),originalRequirements:z.array(requirementIdentity).min(1).max(30)}).strict().optional(),
  obligations:z.array(readinessObligationSchema).min(2).max(128)}).strict();
export const candidateReadinessCriterionSchema=z.object({id:z.string().min(1),label:z.string().min(1),
  type:z.literal('candidate_readiness'),version:z.literal(1),name:z.literal('integration-readiness.json'),
  registrationId:z.uuid(),mapDigest:digest,policyRegistrationId:z.uuid(),policyDigest:digest,
  obligationsDigest:digest,localArtifacts:z.array(artifact).min(1).max(20)}).strict();
export const candidateReadinessReportSchema=z.object({version:z.literal(1),state:z.literal('ready_for_ci'),
  localChecks:z.array(z.object({id,status:z.literal('passed'),name:artifact,digest}).strict()).min(1).max(128),
  controllerChecks:z.object({profile:z.literal('agentos.validate'),status:z.literal('pending_controller_ci'),
    obligationsDigest:digest,tests:z.array(z.object({file:z.string(),name:z.string()}).strict()).min(1).max(128)}).strict(),
  blockingFailures:z.array(z.string()).length(0),limitations:z.array(z.string().max(2000)).max(30)}).strict();
export type CandidateReadinessMap=z.infer<typeof candidateReadinessMapSchema>;
export type CandidateReadinessCriterion=z.infer<typeof candidateReadinessCriterionSchema>;
