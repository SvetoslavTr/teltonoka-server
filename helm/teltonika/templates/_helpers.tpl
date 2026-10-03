{{- define "teltonika.fullname" -}}
{{- if contains .Chart.Name .Release.Name }}{{ .Release.Name | trunc 50 | trimSuffix "-" }}{{ else }}{{ printf "%s-%s" .Release.Name .Chart.Name | trunc 50 | trimSuffix "-" }}{{ end }}
{{- end -}}

{{- define "teltonika.labels" -}}
helm.sh/chart: {{ printf "%s-%s" .Chart.Name .Chart.Version | quote }}
app.kubernetes.io/managed-by: {{ .Release.Service }}
app.kubernetes.io/instance: {{ .Release.Name }}
app.kubernetes.io/version: {{ .Values.server.image.tag | quote }}
{{- end -}}

{{- define "teltonika.server.selectorLabels" -}}
app.kubernetes.io/name: teltonika-server
app.kubernetes.io/instance: {{ .Release.Name }}
{{- end -}}
