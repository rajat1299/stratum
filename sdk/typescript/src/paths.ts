export function normalizeRoutePath(path: string): string {
  const parts: string[] = [];
  for (const part of stripLeadingSlash(path).split("/")) {
    if (part === "" || part === ".") {
      continue;
    }
    if (part === "..") {
      parts.pop();
      continue;
    }
    parts.push(part);
  }

  return parts.join("/");
}

export function pathRoute(prefix: "fs" | "tree", path: string): string {
  const routePath = normalizeRoutePath(path);
  if (routePath === "") {
    return prefix;
  }

  return `${prefix}/${encodePathSegments(routePath)}`;
}

export function fsRoute(path: string): string {
  return pathRoute("fs", path);
}

export function treeRoute(path: string): string {
  return pathRoute("tree", path);
}

export function refRoute(name: string): string {
  return `vcs/refs/${encodeRefNameSegments(name)}`;
}

export function encodePathSegments(path: string): string {
  return path.split("/").filter(Boolean).map(encodeURIComponent).join("/");
}

export function encodeRouteSegment(value: string): string {
  return encodeURIComponent(value);
}

function encodeRefNameSegments(name: string): string {
  return name.split("/").map(encodeRefNameSegment).join("/");
}

function encodeRefNameSegment(segment: string): string {
  if (segment === ".") return "%252E";
  if (segment === "..") return "%252E%252E";
  return encodeURIComponent(segment);
}

function stripLeadingSlash(value: string): string {
  return value.replace(/^\/+/, "");
}
