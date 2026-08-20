import { ProjectFormValues } from "./ProjectBaseForm";

type ModelLimitEntry = NonNullable<ProjectFormValues["modelLimits"]>[number];

const buildModelLimitMap = (
  entries: ProjectFormValues["modelLimits"],
  getValue: (entry: ModelLimitEntry) => number | undefined,
): Record<string, number> =>
  Object.fromEntries(
    (entries ?? []).flatMap((entry) => {
      const value = getValue(entry);
      return entry.model && value != null ? [[entry.model, value] as const] : [];
    }),
  );

/**
 * Transforms ProjectFormValues into the flat API param shape
 * shared by both create and update endpoints.
 */
export function buildProjectApiParams(values: ProjectFormValues) {
  const modelRpmLimit = buildModelLimitMap(values.modelLimits, (entry) => entry.rpm);
  const modelTpmLimit = buildModelLimitMap(values.modelLimits, (entry) => entry.tpm);
  const modelItpmLimit = buildModelLimitMap(values.modelLimits, (entry) => entry.itpm);
  const modelOtpmLimit = buildModelLimitMap(values.modelLimits, (entry) => entry.otpm);

  const metadata: Record<string, unknown> = {};
  for (const entry of values.metadata ?? []) {
    if (entry.key) metadata[entry.key] = entry.value;
  }

  return {
    project_alias: values.project_alias,
    description: values.description,
    models: values.models ?? [],
    max_budget: values.max_budget === undefined ? undefined : Math.round(values.max_budget * 100) / 100,
    blocked: values.isBlocked ?? false,
    ...(values.guardrails &&
      values.guardrails.length > 0 && {
        guardrails: values.guardrails,
      }),
    ...(Object.keys(modelRpmLimit).length > 0 && {
      model_rpm_limit: modelRpmLimit,
    }),
    ...(Object.keys(modelTpmLimit).length > 0 && {
      model_tpm_limit: modelTpmLimit,
    }),
    ...(Object.keys(modelItpmLimit).length > 0 && {
      model_itpm_limit: modelItpmLimit,
    }),
    ...(Object.keys(modelOtpmLimit).length > 0 && {
      model_otpm_limit: modelOtpmLimit,
    }),
    ...(Object.keys(metadata).length > 0 && { metadata }),
  };
}
