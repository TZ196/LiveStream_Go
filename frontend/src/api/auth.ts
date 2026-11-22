import { isRecord, request } from './http'

export type User = {
  id: string
  username: string
  created_at: string
}

export type LoginResult = {
  access_token: string
  token_type: 'Bearer'
  expires_in: number
  user: User
}

export type Credentials = { username: string; password: string }

function isUser(value: unknown): value is User {
  return isRecord(value)
    && typeof value.id === 'string' && /^[1-9][0-9]*$/.test(value.id)
    && typeof value.username === 'string' && /^[a-z][a-z0-9_]{2,31}$/.test(value.username)
    && typeof value.created_at === 'string' && !Number.isNaN(Date.parse(value.created_at))
}

function readData(response: unknown): unknown {
  if (!isRecord(response) || typeof response.trace_id !== 'string' || !('data' in response)) {
    throw new Error('账号接口响应格式不正确。')
  }
  return response.data
}

export async function register(input: Credentials, signal: AbortSignal): Promise<User> {
  const response = await request('/api/v1/auth/register', { method: 'POST', body: input, signal })
  const data = readData(response)
  if (!isUser(data)) {
    throw new Error('注册结果格式不正确，请尝试登录确认账号是否已创建。')
  }
  return data
}

export async function login(input: Credentials, signal: AbortSignal): Promise<LoginResult> {
  const response = await request('/api/v1/auth/login', { method: 'POST', body: input, signal })
  const data = readData(response)
  if (!isRecord(data) || typeof data.access_token !== 'string' || data.access_token.length === 0
    || data.token_type !== 'Bearer' || data.expires_in !== 1800 || !isUser(data.user)) {
    throw new Error('登录结果格式不正确，请重新登录。')
  }
  return { access_token: data.access_token, token_type: 'Bearer', expires_in: data.expires_in, user: data.user }
}

export async function currentUser(token: string, signal: AbortSignal): Promise<User> {
  const response = await request('/api/v1/users/me', { token, signal })
  const data = readData(response)
  if (!isUser(data)) {
    throw new Error('账号信息格式不正确，请稍后再试。')
  }
  return data
}
