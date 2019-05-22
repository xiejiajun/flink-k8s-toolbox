package com.nextbreakpoint.model;

import com.google.gson.annotations.SerializedName;

import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;

public class V1FlinkClusterSpec {
    @SerializedName("clusterName")
    private String clusterName;
    @SerializedName("environment")
    private String environment;
    @SerializedName("pullSecrets")
    private String pullSecrets;
    @SerializedName("pullPolicy")
    private String pullPolicy;
    @SerializedName("flinkImage")
    private String flinkImage;
    @SerializedName("serviceMode")
    private String serviceMode;
    @SerializedName("jobmanagerCpus")
    private Float jobmanagerCpus;
    @SerializedName("jobmanagerMemory")
    private Integer jobmanagerMemory;
    @SerializedName("jobmanagerStorageSize")
    private Integer jobmanagerStorageSize;
    @SerializedName("jobmanagerStorageClass")
    private String jobmanagerStorageClass;
    @SerializedName("jobmanagerServiceAccount")
    private String jobmanagerServiceAccount;
    @SerializedName("jobmanagerEnvironmentVariables")
    private List<V1FlinkClusterEnvVar> jobmanagerEnvironmentVariables;
    @SerializedName("taskmanagerCpus")
    private Float taskmanagerCpus;
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
    @SerializedName("taskmanagerEnvironmentVariables")
    private List<V1FlinkClusterEnvVar> taskmanagerEnvironmentVariables;
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

    public Float getJobmanagerCpus() {
        return jobmanagerCpus;
    }

    public V1FlinkClusterSpec setJobmanagerCpus(Float jobmanagerCpus) {
        this.jobmanagerCpus = jobmanagerCpus;
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

    public Float getTaskmanagerCpus() {
        return taskmanagerCpus;
    }

    public V1FlinkClusterSpec setTaskmanagerCpus(Float taskmanagerCpus) {
        this.taskmanagerCpus = taskmanagerCpus;
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

    public void setTaskmanagerTaskSlots(Integer taskmanagerTaskSlots) {
        this.taskmanagerTaskSlots = taskmanagerTaskSlots;
    }

    public String getClusterName() {
        return clusterName;
    }

    public V1FlinkClusterSpec setClusterName(String clusterName) {
        this.clusterName = clusterName;
        return this;
    }

    public String getEnvironment() {
        return environment;
    }

    public V1FlinkClusterSpec setEnvironment(String environment) {
        this.environment = environment;
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

    public List<V1FlinkClusterEnvVar> getJobmanagerEnvironmentVariables() {
        return jobmanagerEnvironmentVariables;
    }

    public V1FlinkClusterSpec setJobmanagerEnvironmentVariables(List<V1FlinkClusterEnvVar> jobmanagerEnvironmentVariables) {
        this.jobmanagerEnvironmentVariables = jobmanagerEnvironmentVariables;
        return this;
    }

    public List<V1FlinkClusterEnvVar> getTaskmanagerEnvironmentVariables() {
        return taskmanagerEnvironmentVariables;
    }

    public V1FlinkClusterSpec setTaskmanagerEnvironmentVariables(List<V1FlinkClusterEnvVar> taskmanagerEnvironmentVariables) {
        this.taskmanagerEnvironmentVariables = taskmanagerEnvironmentVariables;
        return this;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        V1FlinkClusterSpec that = (V1FlinkClusterSpec) o;
        return Objects.equals(clusterName, that.clusterName) &&
                Objects.equals(environment, that.environment) &&
                Objects.equals(pullSecrets, that.pullSecrets) &&
                Objects.equals(pullPolicy, that.pullPolicy) &&
                Objects.equals(flinkImage, that.flinkImage) &&
                Objects.equals(serviceMode, that.serviceMode) &&
                Objects.equals(jobmanagerCpus, that.jobmanagerCpus) &&
                Objects.equals(jobmanagerMemory, that.jobmanagerMemory) &&
                Objects.equals(jobmanagerStorageSize, that.jobmanagerStorageSize) &&
                Objects.equals(jobmanagerStorageClass, that.jobmanagerStorageClass) &&
                Objects.equals(jobmanagerServiceAccount, that.jobmanagerServiceAccount) &&
                Objects.equals(jobmanagerEnvironmentVariables, that.jobmanagerEnvironmentVariables) &&
                Objects.equals(taskmanagerCpus, that.taskmanagerCpus) &&
                Objects.equals(taskmanagerMemory, that.taskmanagerMemory) &&
                Objects.equals(taskmanagerStorageSize, that.taskmanagerStorageSize) &&
                Objects.equals(taskmanagerStorageClass, that.taskmanagerStorageClass) &&
                Objects.equals(taskmanagerReplicas, that.taskmanagerReplicas) &&
                Objects.equals(taskmanagerTaskSlots, that.taskmanagerTaskSlots) &&
                Objects.equals(taskmanagerServiceAccount, that.taskmanagerServiceAccount) &&
                Objects.equals(taskmanagerEnvironmentVariables, that.taskmanagerEnvironmentVariables) &&
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
        return Objects.hash(clusterName, environment, pullSecrets, pullPolicy, flinkImage, serviceMode, jobmanagerCpus, jobmanagerMemory, jobmanagerStorageSize, jobmanagerStorageClass, jobmanagerServiceAccount, jobmanagerEnvironmentVariables, taskmanagerCpus, taskmanagerMemory, taskmanagerStorageSize, taskmanagerStorageClass, taskmanagerReplicas, taskmanagerTaskSlots, taskmanagerServiceAccount, taskmanagerEnvironmentVariables, sidecarImage, sidecarClassName, sidecarJarPath, sidecarArguments, sidecarServiceAccount, sidecarSavepoint, sidecarParallelism);
    }

    @Override
    public String toString() {
        return "V1FlinkClusterSpec {" +
                "clusterName='" + clusterName + '\'' +
                ", environment='" + environment + '\'' +
                ", pullSecrets='" + pullSecrets + '\'' +
                ", pullPolicy='" + pullPolicy + '\'' +
                ", flinkImage='" + flinkImage + '\'' +
                ", serviceMode='" + serviceMode + '\'' +
                ", jobmanagerCpus=" + jobmanagerCpus +
                ", jobmanagerMemory=" + jobmanagerMemory +
                ", jobmanagerStorageSize=" + jobmanagerStorageSize +
                ", jobmanagerStorageClass='" + jobmanagerStorageClass + '\'' +
                ", jobmanagerServiceAccount='" + jobmanagerServiceAccount + '\'' +
                ", jobmanagerEnvironmentVariables=" + jobmanagerEnvironmentVariables.stream().map(V1FlinkClusterEnvVar::toString).collect(Collectors.joining(",")) +
                ", taskmanagerCpus=" + taskmanagerCpus +
                ", taskmanagerMemory=" + taskmanagerMemory +
                ", taskmanagerStorageSize=" + taskmanagerStorageSize +
                ", taskmanagerStorageClass='" + taskmanagerStorageClass + '\'' +
                ", taskmanagerReplicas=" + taskmanagerReplicas +
                ", taskmanagerTaskSlots=" + taskmanagerTaskSlots +
                ", taskmanagerServiceAccount='" + taskmanagerServiceAccount + '\'' +
                ", taskmanagerEnvironmentVariables=" + taskmanagerEnvironmentVariables.stream().map(V1FlinkClusterEnvVar::toString).collect(Collectors.joining(",")) +
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
