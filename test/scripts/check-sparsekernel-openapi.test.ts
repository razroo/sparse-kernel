import { describe, expect, it } from "vitest";
import {
  collectOpenApiInlineRequestBodyRoutes,
  collectOpenApiRequestBodySchemaNames,
} from "../../scripts/check-sparsekernel-openapi.mjs";

describe("scripts/check-sparsekernel-openapi", () => {
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
