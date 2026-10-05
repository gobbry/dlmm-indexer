import package_json from "../package.json" with { type: "json" };

export const TOOL_NAME = "metclanker";
export const TOOL_VERSION: string = package_json.version;
