{{/*
Expand the name of the chart.
*/}}
{{- define "teltonika.name" -}}
{{- default .Chart.Name .Values.nameOverride | trunc 63 | trimSuffix "-" }}
{{- end }}

{{/*
Create a default fully qualified app name.
*/}}
{{- define "teltonika.fullname" -}}
{{- $name := default .Chart.Name .Values.nameOverride }}
{{- if contains $name .Release.Name }}
{{- .Release.Name | trunc 63 | trimSuffix "-" }}
{{- else }}
{{- printf "%s-%s" .Release.Name $name | trunc 63 | trimSuffix "-" }}
{{- end }}
{{- end }}

{{/*
Common labels
*/}}
{{- define "teltonika.labels" -}}
helm.sh/chart: {{ printf "%s-%s" .Chart.Name .Chart.Version | quote }}
app.kubernetes.io/managed-by: {{ .Release.Service }}
app.kubernetes.io/instance: {{ .Release.Name }}
{{- end }}

{{/*
Selector labels for the TCP server
*/}}
{{- define "teltonika.server.selectorLabels" -}}
app.kubernetes.io/name: {{ include "teltonika.name" . }}-server
app.kubernetes.io/instance: {{ .Release.Name }}
{{- end }}

{{/*
Selector labels for the sync worker
*/}}
{{- define "teltonika.sync.selectorLabels" -}}
app.kubernetes.io/name: {{ include "teltonika.name" . }}-sync
app.kubernetes.io/instance: {{ .Release.Name }}
{{- end }}

{{/*
Name of the shared Secret
*/}}
{{- define "teltonika.secretName" -}}
{{ include "teltonika.fullname" . }}-credentials
{{- end }}

{{/*
Name of the shared ConfigMap
*/}}
{{- define "teltonika.configmapName" -}}
{{ include "teltonika.fullname" . }}-config
{{- end }}

{{/*
Name of the sync worker PVC
*/}}
{{- define "teltonika.sync.pvcName" -}}
{{ include "teltonika.fullname" . }}-sync-watermark
{{- end }}

{{/*
MongoDB hostname — auto-derived from the Bitnami subchart service name when
mongodb.enabled is true, or taken from mongodb.host for an external instance.
*/}}
{{- define "teltonika.mongoHost" -}}
{{- if .Values.mongodb.enabled -}}
{{- printf "%s-mongodb" .Release.Name -}}
{{- else -}}
{{- required "mongodb.host is required when mongodb.enabled is false" .Values.mongodb.host -}}
{{- end -}}
{{- end }}

{{/*
MongoDB username — single source of truth. When the subchart is enabled the
app must use the same user the subchart provisions (mongodb.auth.username);
otherwise it uses credentials.mongoUser for the external instance.
*/}}
{{- define "teltonika.mongoUser" -}}
{{- if .Values.mongodb.enabled -}}
{{- .Values.mongodb.auth.username -}}
{{- else -}}
{{- .Values.credentials.mongoUser -}}
{{- end -}}
{{- end }}

{{/*
MongoDB password — required (never silently empty). When the subchart is
enabled the app reuses mongodb.auth.password so the two can never drift apart;
otherwise it requires credentials.mongoPass for the external instance.
*/}}
{{- define "teltonika.mongoPass" -}}
{{- if .Values.mongodb.enabled -}}
{{- required "mongodb.auth.password is required when mongodb.enabled is true — pass it with --set mongodb.auth.password=..." .Values.mongodb.auth.password -}}
{{- else -}}
{{- required "credentials.mongoPass is required when using an external MongoDB — pass it with --set credentials.mongoPass=..." .Values.credentials.mongoPass -}}
{{- end -}}
{{- end }}
