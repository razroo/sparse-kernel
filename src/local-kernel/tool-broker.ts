import type { AgentToolUpdateCallback } from "@mariozechner/pi-agent-core";
import type { AnyAgentTool } from "../agents/tools/common.js";
import { copyPluginToolMeta, getPluginToolMeta } from "../plugins/tools.js";
import { LocalBrowserBroker } from "./browser-broker.js";
import type { LocalKernelDatabase } from "./database.js";

export type ToolBrokerSubject = {
  subjectType: string;
  subjectId: string;
};

export type ToolBrokerContext = {
  agentId?: string;
  sessionId?: string;
  taskId?: string;
  runId?: string;
  subject: ToolBrokerSubject;
};

export class CapabilityToolBroker {
  constructor(private readonly db: LocalKernelDatabase) {}

  wrapTool(tool: AnyAgentTool, context: ToolBrokerContext): AnyAgentTool {
    const execute = tool.execute;
    const wrapped: AnyAgentTool = {
      ...tool,
      execute: async (
        toolCallId: string,
        params: unknown,
        signal?: AbortSignal,
        onUpdate?: AgentToolUpdateCallback<unknown>,
      ) => {
        const providerToolCallId = toolCallId.trim() || `tool_call:${Date.now()}`;
        const toolCallDbId = this.db.insertToolCall({
          id: context.runId ? `${context.runId}:${providerToolCallId}` : providerToolCallId,
          taskId: context.taskId,
          sessionId: context.sessionId,
          agentId: context.agentId,
          toolName: tool.name,
          input: { providerToolCallId, params },
        });
        const pluginMeta = getPluginToolMeta(tool);
        const allowed = this.db.checkCapability({
          subjectType: context.subject.subjectType,
          subjectId: context.subject.subjectId,
          resourceType: "tool",
          resourceId: tool.name,
          action: "invoke",
          context: { pluginId: pluginMeta?.pluginId, taskId: context.taskId },
        });
        if (!allowed) {
          const err = new Error(`Tool invocation denied: ${tool.name}`);
          this.db.failToolCall(toolCallDbId, err);
          throw err;
        }
        this.db.startToolCall(toolCallDbId);
        const browserContext = this.maybeAcquireBrowserContext(tool, context, params);
        try {
          const result = await execute(toolCallId, params, signal, onUpdate);
          this.db.finishToolCall(toolCallDbId, result);
          return result;
        } catch (err) {
          this.db.failToolCall(toolCallDbId, err);
          throw err;
        } finally {
          if (browserContext) {
            new LocalBrowserBroker(this.db).releaseContext(browserContext.id);
          }
        }
      },
    };
    copyPluginToolMeta(tool, wrapped);
    return wrapped;
  }

  wrapTools(tools: AnyAgentTool[], context: ToolBrokerContext): AnyAgentTool[] {
    return tools.map((tool) => this.wrapTool(tool, context));
  }

  private maybeAcquireBrowserContext(
    tool: AnyAgentTool,
    context: ToolBrokerContext,
    params: unknown,
  ) {
    if (tool.name !== "browser" || !context.agentId) {
      return null;
    }
    const record = params && typeof params === "object" ? (params as Record<string, unknown>) : {};
    const profile = typeof record.profile === "string" ? record.profile.trim().toLowerCase() : "";
    const target = typeof record.target === "string" ? record.target.trim().toLowerCase() : "";
    const trustZoneId =
      profile === "user"
        ? "user_browser_profile"
        : profile || target === "host"
          ? "authenticated_web"
          : "public_web";
    const action = typeof record.action === "string" ? record.action.trim() : "";
    const urlCandidate =
      typeof record.targetUrl === "string"
        ? record.targetUrl
        : typeof record.url === "string"
          ? record.url
          : undefined;
    const shouldEnforceNetwork =
      process.env.OPENCLAW_RUNTIME_BROWSER_POLICY_ENFORCE === "1" ||
      process.env.OPENCLAW_RUNTIME_BROWSER_POLICY_ENFORCE === "true";
    const allowedOrigins =
      shouldEnforceNetwork && urlCandidate && (action === "open" || action === "navigate")
        ? [urlCandidate]
        : undefined;
    try {
      return new LocalBrowserBroker(this.db).acquireContext({
        agentId: context.agentId,
        sessionId: context.sessionId,
        taskId: context.taskId,
        trustZoneId,
        profileMode: profile === "user" ? "user" : "ephemeral",
        allowedOrigins,
      });
    } catch (err) {
      this.db.recordAudit({
        actor: { type: "agent", id: context.agentId },
        action: "browser_context.accounting_failed",
        objectType: "tool",
        objectId: tool.name,
        payload: {
          trustZoneId,
          error: err instanceof Error ? err.message : String(err),
        },
      });
      if (shouldEnforceNetwork) {
        throw err;
      }
      return null;
    }
  }
}
