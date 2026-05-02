import { describe, expect, it } from "vitest";
import {
  collectClientSchemaMappingProblems,
  collectOpenApiInlineArrayResponseItemRoutes,
  collectOpenApiInlineObjectResponseSchemaRoutes,
  collectOpenApiInlineRequestBodyRoutes,
  collectOpenApiMissingJsonResponseSchemaRoutes,
  collectOpenApiOperationIdProblems,
  collectOpenApiReferencedSchemaNames,
  collectOpenApiRequestBodySchemaNames,
} from "../../scripts/check-sparsekernel-openapi.mjs";

describe("scripts/check-sparsekernel-openapi", () => {
  it("finds duplicate client parity mappings", () => {
    expect(
      collectClientSchemaMappingProblems([
        { clientType: "SparseKernelTask", schemaName: "Task" },
        { clientType: "SparseKernelSession", schemaName: "Session" },
        { clientType: "SparseKernelTask", schemaName: "TaskInput" },
        { clientType: "SparseKernelTaskInput", schemaName: "Task" },
      ]),
    ).toEqual({
      duplicateClientTypes: ["SparseKernelTask"],
      duplicateSchemaNames: ["Task"],
    });
  });

  it("collects component-backed request body schema names", () => {
    const paths = {
      "/tasks/enqueue": {
        post: {
          requestBody: {
            content: {
              "application/json": {
                schema: { $ref: "#/components/schemas/EnqueueTaskInput" },
              },
            },
          },
        },
      },
      "/health": {
        get: {},
      },
    };

    expect([...collectOpenApiRequestBodySchemaNames(paths)]).toEqual(["EnqueueTaskInput"]);
  });

  it("collects all referenced component schemas for client parity coverage", () => {
    const openapi = {
      paths: {
        "/tasks": {
          get: {
            responses: {
              "200": {
                content: {
                  "application/json": {
                    schema: {
                      type: "array",
                      items: { $ref: "#/components/schemas/Task" },
                    },
                  },
                },
              },
            },
          },
        },
        "/tasks/enqueue": {
          post: {
            requestBody: {
              content: {
                "application/json": {
                  schema: { $ref: "#/components/schemas/EnqueueTaskInput" },
                },
              },
            },
          },
        },
      },
    };

    expect([...collectOpenApiReferencedSchemaNames(openapi)].sort()).toEqual([
      "EnqueueTaskInput",
      "Task",
    ]);
  });

  it("finds missing and duplicate operation ids", () => {
    const paths = {
      "/tasks": {
        get: { operationId: "listTasks" },
      },
      "/tasks/enqueue": {
        post: { operationId: "listTasks" },
      },
      "/tasks/claim": {
        post: {},
      },
    };

    expect(collectOpenApiOperationIdProblems(paths)).toEqual({
      duplicateOperationIds: ["listTasks: GET /tasks, POST /tasks/enqueue"],
      missingOperationIds: ["POST /tasks/claim"],
    });
  });

  it("finds operations missing 200 JSON response schemas", () => {
    const paths = {
      "/health": {
        get: {
          responses: {
            "200": {
              content: {
                "application/json": {
                  schema: {
                    type: "object",
                    properties: { ok: { type: "boolean" } },
                  },
                },
              },
            },
          },
        },
      },
      "/tasks": {
        get: {
          responses: {
            "200": {
              description: "Missing schema",
            },
          },
        },
      },
    };

    expect(collectOpenApiMissingJsonResponseSchemaRoutes(paths)).toEqual(["GET /tasks"]);
  });

  it("finds inline object response schemas that bypass client parity mappings", () => {
    const paths = {
      "/health": {
        get: {
          responses: {
            "200": {
              content: {
                "application/json": {
                  schema: {
                    type: "object",
                    properties: { ok: { type: "boolean" } },
                  },
                },
              },
            },
          },
        },
      },
      "/tasks": {
        get: {
          responses: {
            "200": {
              content: {
                "application/json": {
                  schema: {
                    type: "array",
                    items: { $ref: "#/components/schemas/Task" },
                  },
                },
              },
            },
          },
        },
      },
    };

    expect(collectOpenApiInlineObjectResponseSchemaRoutes(paths)).toEqual(["GET /health"]);
  });

  it("finds inline array response item schemas that bypass client parity mappings", () => {
    const paths = {
      "/tasks": {
        get: {
          responses: {
            "200": {
              content: {
                "application/json": {
                  schema: {
                    type: "array",
                    items: {
                      type: "object",
                      properties: { id: { type: "string" } },
                    },
                  },
                },
              },
            },
          },
        },
      },
      "/sessions": {
        get: {
          responses: {
            "200": {
              content: {
                "application/json": {
                  schema: {
                    type: "array",
                    items: { $ref: "#/components/schemas/Session" },
                  },
                },
              },
            },
          },
        },
      },
    };

    expect(collectOpenApiInlineArrayResponseItemRoutes(paths)).toEqual(["GET /tasks"]);
  });

  it("finds inline request body schemas that bypass client parity mappings", () => {
    const paths = {
      "/leases/release-expired": {
        post: {
          requestBody: {
            content: {
              "application/json": {
                schema: {
                  type: "object",
                  properties: {
                    now: { type: "string" },
                  },
                },
              },
            },
          },
        },
      },
    };

    expect([...collectOpenApiInlineRequestBodyRoutes(paths)]).toEqual([
      "POST /leases/release-expired",
    ]);
  });
});
