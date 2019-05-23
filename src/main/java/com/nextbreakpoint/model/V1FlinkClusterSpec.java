package com.nextbreakpoint.model;

import com.google.gson.annotations.SerializedName;

import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;

public class V1FlinkClusterSpec {
    @SerializedName("pullSecrets")
    private String pullSecrets;
    @SerializedName("pullPolicy")
    private String pullPolicy;
    @SerializedName("flinkImage")
    private String flinkImage;
    @SerializedName("serviceMode")
    private String serviceMode;
    @SerializedName("jobmanagerCPUs")
    private Float jobmanagerCPUs;
    @SerializedName("jobmanagerMemory")
    private Integer jobmanagerMemory;
    @SerializedName("jobmanagerStorageSize")
    private Integer jobmanagerStorageSize;
    @SerializedName("jobmanagerStorageClass")
    private String jobmanagerStorageClass;
    @SerializedName("jobmanagerServiceAccount")
    private String jobmanagerServiceAccount;
    @SerializedName("jobmanagerEnvironment")
    private List<V1FlinkClusterEnvVar> jobmanagerEnvironment;
    @SerializedName("taskmanagerCPUs")
    private Float taskmanagerCPUs;
    @SerializedName("taskmanagerMemory")
    private Integer taskmanagerMemory;
    @SerializedName("taskmanagerStorageSize")
    private Integer taskmanagerStorageSize;
    @SerializedName("taskmanagerStorageClass")
    private String taskmanagerStorageClass;
    @SerializedName("taskmanagerReplicas")
    private Integer taskmanagerReplicas;
    @SerializedName("taskmanagerTaskSlots")
    private Integer taskmanagerTaskSlots;
    @SerializedName("taskmanagerServiceAccount")
    private String taskmanagerServiceAccount;
    @SerializedName("taskmanagerEnvironment")
    private List<V1FlinkClusterEnvVar> taskmanagerEnvironment;
    @SerializedName("sidecarImage")
    private String sidecarImage;
    @SerializedName("sidecarClassName")
    private String sidecarClassName;
    @SerializedName("sidecarJarPath")
    private String sidecarJarPath;
    @SerializedName("sidecarArguments")
    private List<String> sidecarArguments;
    @SerializedName("sidecarServiceAccount")
    private String sidecarServiceAccount;
    @SerializedName("sidecarSavepoint")
    private String sidecarSavepoint;
    @SerializedName("sidecarParallelism")
    private Integer sidecarParallelism;

    public String getPullPolicy() {
        return pullPolicy;
    }

    public V1FlinkClusterSpec setPullPolicy(String pullPolicy) {
        this.pullPolicy = pullPolicy;
        return this;
    }

    public String getServiceMode() {
        return serviceMode;
    }

    public V1FlinkClusterSpec setServiceMode(String serviceMode) {
        this.serviceMode = serviceMode;
        return this;
    }

    public Float getJobmanagerCPUs() {
        return jobmanagerCPUs;
    }

    public V1FlinkClusterSpec setJobmanagerCPUs(Float jobmanagerCPUs) {
        this.jobmanagerCPUs = jobmanagerCPUs;
        return this;
    }

    public Integer getJobmanagerMemory() {
        return jobmanagerMemory;
    }

    public V1FlinkClusterSpec setJobmanagerMemory(Integer jobmanagerMemory) {
        this.jobmanagerMemory = jobmanagerMemory;
        return this;
    }

    public Integer getJobmanagerStorageSize() {
        return jobmanagerStorageSize;
    }

    public V1FlinkClusterSpec setJobmanagerStorageSize(Integer jobmanagerStorageSize) {
        this.jobmanagerStorageSize = jobmanagerStorageSize;
        return this;
    }

    public String getJobmanagerStorageClass() {
        return jobmanagerStorageClass;
    }

    public V1FlinkClusterSpec setJobmanagerStorageClass(String jobmanagerStorageClass) {
        this.jobmanagerStorageClass = jobmanagerStorageClass;
        return this;
    }

    public Float getTaskmanagerCPUs() {
        return taskmanagerCPUs;
    }

    public V1FlinkClusterSpec setTaskmanagerCPUs(Float taskmanagerCPUs) {
        this.taskmanagerCPUs = taskmanagerCPUs;
        return this;
    }

    public Integer getTaskmanagerMemory() {
        return taskmanagerMemory;
    }

    public V1FlinkClusterSpec setTaskmanagerMemory(Integer taskmanagerMemory) {
        this.taskmanagerMemory = taskmanagerMemory;
        return this;
    }

    public Integer getTaskmanagerStorageSize() {
        return taskmanagerStorageSize;
    }

    public V1FlinkClusterSpec setTaskmanagerStorageSize(Integer taskmanagerStorageSize) {
        this.taskmanagerStorageSize = taskmanagerStorageSize;
        return this;
    }

    public String getTaskmanagerStorageClass() {
        return taskmanagerStorageClass;
    }

    public V1FlinkClusterSpec setTaskmanagerStorageClass(String taskmanagerStorageClass) {
        this.taskmanagerStorageClass = taskmanagerStorageClass;
        return this;
    }

    public Integer getTaskmanagerReplicas() {
        return taskmanagerReplicas;
    }

    public V1FlinkClusterSpec setTaskmanagerReplicas(Integer taskmanagerReplicas) {
        this.taskmanagerReplicas = taskmanagerReplicas;
        return this;
    }

    public Integer getTaskmanagerTaskSlots() {
        return taskmanagerTaskSlots;
    }

    public V1FlinkClusterSpec setTaskmanagerTaskSlots(Integer taskmanagerTaskSlots) {
        this.taskmanagerTaskSlots = taskmanagerTaskSlots;
        return this;
    }

    public String getPullSecrets() {
        return pullSecrets;
    }

    public V1FlinkClusterSpec setPullSecrets(String pullSecrets) {
        this.pullSecrets = pullSecrets;
        return this;
    }

    public String getFlinkImage() {
        return flinkImage;
    }

    public V1FlinkClusterSpec setFlinkImage(String flinkImage) {
        this.flinkImage = flinkImage;
        return this;
    }

    public String getSidecarImage() {
        return sidecarImage;
    }

    public V1FlinkClusterSpec setSidecarImage(String sidecarImage) {
        this.sidecarImage = sidecarImage;
        return this;
    }

    public List<String> getSidecarArguments() {
        return sidecarArguments;
    }

    public V1FlinkClusterSpec setSidecarArguments(List<String> sidecarArguments) {
        this.sidecarArguments = sidecarArguments;
        return this;
    }

    public String getJobmanagerServiceAccount() {
        return jobmanagerServiceAccount;
    }

    public V1FlinkClusterSpec setJobmanagerServiceAccount(String jobmanagerServiceAccount) {
        this.jobmanagerServiceAccount = jobmanagerServiceAccount;
        return this;
    }

    public String getTaskmanagerServiceAccount() {
        return taskmanagerServiceAccount;
    }

    public V1FlinkClusterSpec setTaskmanagerServiceAccount(String taskmanagerServiceAccount) {
        this.taskmanagerServiceAccount = taskmanagerServiceAccount;
        return this;
    }

    public String getSidecarServiceAccount() {
        return sidecarServiceAccount;
    }

    public V1FlinkClusterSpec setSidecarServiceAccount(String sidecarServiceAccount) {
        this.sidecarServiceAccount = sidecarServiceAccount;
        return this;
    }

    public String getSidecarClassName() {
        return sidecarClassName;
    }

    public V1FlinkClusterSpec setSidecarClassName(String sidecarClassName) {
        this.sidecarClassName = sidecarClassName;
        return this;
    }

    public String getSidecarJarPath() {
        return sidecarJarPath;
    }

    public V1FlinkClusterSpec setSidecarJarPath(String sidecarJarPath) {
        this.sidecarJarPath = sidecarJarPath;
        return this;
    }

    public String getSidecarSavepoint() {
        return sidecarSavepoint;
    }

    public V1FlinkClusterSpec setSidecarSavepoint(String sidecarSavepoint) {
        this.sidecarSavepoint = sidecarSavepoint;
        return this;
    }

    public Integer getSidecarParallelism() {
        return sidecarParallelism;
    }

    public V1FlinkClusterSpec setSidecarParallelism(Integer sidecarParallelism) {
        this.sidecarParallelism = sidecarParallelism;
        return this;
    }

    public List<V1FlinkClusterEnvVar> getJobmanagerEnvironment() {
        return jobmanagerEnvironment;
    }

    public V1FlinkClusterSpec setJobmanagerEnvironment(List<V1FlinkClusterEnvVar> jobmanagerEnvironment) {
        this.jobmanagerEnvironment = jobmanagerEnvironment;
        return this;
    }

    public List<V1FlinkClusterEnvVar> getTaskmanagerEnvironment() {
        return taskmanagerEnvironment;
    }

    public V1FlinkClusterSpec setTaskmanagerEnvironment(List<V1FlinkClusterEnvVar> taskmanagerEnvironment) {
        this.taskmanagerEnvironment = taskmanagerEnvironment;
        return this;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        V1FlinkClusterSpec that = (V1FlinkClusterSpec) o;
        return Objects.equals(pullSecrets, that.pullSecrets) &&
                Objects.equals(pullPolicy, that.pullPolicy) &&
                Objects.equals(flinkImage, that.flinkImage) &&
                Objects.equals(serviceMode, that.serviceMode) &&
                Objects.equals(jobmanagerCPUs, that.jobmanagerCPUs) &&
                Objects.equals(jobmanagerMemory, that.jobmanagerMemory) &&
                Objects.equals(jobmanagerStorageSize, that.jobmanagerStorageSize) &&
                Objects.equals(jobmanagerStorageClass, that.jobmanagerStorageClass) &&
                Objects.equals(jobmanagerServiceAccount, that.jobmanagerServiceAccount) &&
                Objects.equals(jobmanagerEnvironment, that.jobmanagerEnvironment) &&
                Objects.equals(taskmanagerCPUs, that.taskmanagerCPUs) &&
                Objects.equals(taskmanagerMemory, that.taskmanagerMemory) &&
                Objects.equals(taskmanagerStorageSize, that.taskmanagerStorageSize) &&
                Objects.equals(taskmanagerStorageClass, that.taskmanagerStorageClass) &&
                Objects.equals(taskmanagerReplicas, that.taskmanagerReplicas) &&
                Objects.equals(taskmanagerTaskSlots, that.taskmanagerTaskSlots) &&
                Objects.equals(taskmanagerServiceAccount, that.taskmanagerServiceAccount) &&
                Objects.equals(taskmanagerEnvironment, that.taskmanagerEnvironment) &&
                Objects.equals(sidecarImage, that.sidecarImage) &&
                Objects.equals(sidecarClassName, that.sidecarClassName) &&
                Objects.equals(sidecarJarPath, that.sidecarJarPath) &&
                Objects.equals(sidecarArguments, that.sidecarArguments) &&
                Objects.equals(sidecarServiceAccount, that.sidecarServiceAccount) &&
                Objects.equals(sidecarSavepoint, that.sidecarSavepoint) &&
                Objects.equals(sidecarParallelism, that.sidecarParallelism);
    }

    @Override
    public int hashCode() {
        return Objects.hash(pullSecrets, pullPolicy, flinkImage, serviceMode, jobmanagerCPUs, jobmanagerMemory, jobmanagerStorageSize, jobmanagerStorageClass, jobmanagerServiceAccount, jobmanagerEnvironment, taskmanagerCPUs, taskmanagerMemory, taskmanagerStorageSize, taskmanagerStorageClass, taskmanagerReplicas, taskmanagerTaskSlots, taskmanagerServiceAccount, taskmanagerEnvironment, sidecarImage, sidecarClassName, sidecarJarPath, sidecarArguments, sidecarServiceAccount, sidecarSavepoint, sidecarParallelism);
    }

    @Override
    public String toString() {
        return "V1FlinkClusterSpec {" +
                "pullSecrets='" + pullSecrets + '\'' +
                ", pullPolicy='" + pullPolicy + '\'' +
                ", flinkImage='" + flinkImage + '\'' +
                ", serviceMode='" + serviceMode + '\'' +
                ", jobmanagerCPUs=" + jobmanagerCPUs +
                ", jobmanagerMemory=" + jobmanagerMemory +
                ", jobmanagerStorageSize=" + jobmanagerStorageSize +
                ", jobmanagerStorageClass='" + jobmanagerStorageClass + '\'' +
                ", jobmanagerServiceAccount='" + jobmanagerServiceAccount + '\'' +
                ", jobmanagerEnvironment=" + jobmanagerEnvironment.stream().map(V1FlinkClusterEnvVar::toString).collect(Collectors.joining(",")) +
                ", taskmanagerCPUs=" + taskmanagerCPUs +
                ", taskmanagerMemory=" + taskmanagerMemory +
                ", taskmanagerStorageSize=" + taskmanagerStorageSize +
                ", taskmanagerStorageClass='" + taskmanagerStorageClass + '\'' +
                ", taskmanagerReplicas=" + taskmanagerReplicas +
                ", taskmanagerTaskSlots=" + taskmanagerTaskSlots +
                ", taskmanagerServiceAccount='" + taskmanagerServiceAccount + '\'' +
                ", taskmanagerEnvironment=" + taskmanagerEnvironment.stream().map(V1FlinkClusterEnvVar::toString).collect(Collectors.joining(",")) +
                ", sidecarImage='" + sidecarImage + '\'' +
                ", sidecarClassName='" + sidecarClassName + '\'' +
                ", sidecarJarPath='" + sidecarJarPath + '\'' +
                ", sidecarArguments=" + sidecarArguments +
                ", sidecarServiceAccount='" + sidecarServiceAccount + '\'' +
                ", sidecarSavepoint='" + sidecarSavepoint + '\'' +
                ", sidecarParallelism=" + sidecarParallelism +
                '}';
    }
}
