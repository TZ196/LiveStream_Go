export class ApiError extends Error {
  constructor(
    message: string,
    readonly status: number,
    readonly code: string,
    readonly retryable: boolean,
    readonly traceId: string,
    readonly retryAfter: number,
  ) {
    super(message)
    this.name = 'ApiError'
  }
}

// 网络中断或超时不能证明写请求没有成功，交给页面说明如何确认结果。
export class RequestError extends Error {}

export function isRecord(value: unknown): value is Record<string, unknown> {
  return typeof value === 'object' && value !== null && !Array.isArray(value)
}

type RequestOptions = {
  signal: AbortSignal
  method?: 'GET' | 'POST'
  body?: unknown
  token?: string
}

export async function request(path: string, options: RequestOptions): Promise<unknown> {
  const timeoutSignal = AbortSignal.timeout(5000)
  const signal = AbortSignal.any([options.signal, timeoutSignal])
  const headers = new Headers()
  if (options.body !== undefined) {
    headers.set('Content-Type', 'application/json')
  }
  if (options.token) {
    headers.set('Authorization', `Bearer ${options.token}`)
  }

  try {
    const response = await fetch(path, {
      method: options.method ?? 'GET',
      body: options.body === undefined ? undefined : JSON.stringify(options.body),
      headers,
      cache: 'no-store',
      credentials: 'omit',
      signal,
    })
    let data: unknown
    try {
      data = await response.json()
    } catch {
      if (response.ok) {
        throw new Error('服务响应格式不正确，请检查后端地址。')
      }
    }

    if (!response.ok) {
      const seconds = Number(response.headers.get('Retry-After'))
      const retryAfter = Number.isFinite(seconds) && seconds > 0 ? Math.ceil(seconds) : 0
      if (isRecord(data) && isRecord(data.error)) {
        const detail = data.error
        if (typeof detail.message === 'string' && typeof detail.code === 'string') {
          throw new ApiError(detail.message, response.status, detail.code,
            detail.retryable === true, typeof data.trace_id === 'string' ? data.trace_id : '', retryAfter)
        }
      }
      // 请求在进入业务 Handler 前被 Hertz 拒绝时，响应可能是纯文本。
      const message = response.status === 413 ? '请求内容过大，请缩短输入。' : `服务返回 HTTP ${response.status}，请稍后再试。`
      throw new ApiError(message, response.status, 'HTTP_ERROR', false, '', retryAfter)
    }
    return data
  } catch (error) {
    if (options.signal.aborted) {
      throw error
    }
    if (timeoutSignal.aborted) {
      throw new RequestError('请求超时，请稍后确认结果。')
    }
    if (error instanceof TypeError) {
      throw new RequestError('无法连接后端，请确认服务已启动。')
    }
    throw error
  }
}
