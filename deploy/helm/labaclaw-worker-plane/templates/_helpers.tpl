{{- define "labaclaw-worker-plane.name" -}}
{{- default .Chart.Name .Values.fullnameOverride | trunc 63 | trimSuffix "-" -}}
{{- end -}}

{{- define "labaclaw-worker-plane.serviceAccountName" -}}
{{- if .Values.serviceAccount.create -}}
{{- default (include "labaclaw-worker-plane.name" .) .Values.serviceAccount.name -}}
{{- else -}}
{{- default "default" .Values.serviceAccount.name -}}
{{- end -}}
{{- end -}}

