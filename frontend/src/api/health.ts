import { get } from './http'

export async function checkHealth(signal: AbortSignal): Promise<void> {
  const data = await get('/healthz', signal)

  // HTTP 200 还不够，必须核对探针响应，避免把其他页面误认为健康接口。
  if (typeof data !== 'object' || data === null) {
    throw new Error('健康接口响应不符合约定。')
  }
  if (!('status' in data) || data.status !== 'ok') {
    throw new Error('健康接口响应不符合约定。')
  }
}
