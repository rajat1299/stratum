export interface CreateBashOptions {
  baseUrl: string;
  workspaceId: string;
  workspaceToken: string;
}

export interface BashPlaceholder {
  readonly kind: "stratum-bash-placeholder";
}

export interface CreateBashResult {
  readonly options: CreateBashOptions;
  readonly bash: BashPlaceholder;
  readonly toolDescription: string;
  readonly refresh: () => Promise<void>;
}
