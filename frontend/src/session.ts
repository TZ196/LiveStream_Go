import { isRecord } from './api/http'
import type { User } from './api/auth'

export const SESSION_KEY = 'livestream.access-token'

export type Session = { access_token: string; expires_at: number; user: User }

// 这里只读取到期时间来安排界面退出，身份与签名仍由服务端验证。
export function tokenExpiresAt(token: string): number {
  try {
    const parts = token.split('.')
    if (parts.length !== 3 || token.length > 4096) throw new Error()
    const payload = parts[1]!.replace(/-/g, '+').replace(/_/g, '/')
    const claims: unknown = JSON.parse(atob(payload))
    if (isRecord(claims) && typeof claims.exp === 'number' && Number.isSafeInteger(claims.exp)
      && typeof claims.iat === 'number' && Number.isSafeInteger(claims.iat)
      && claims.exp > claims.iat && claims.exp - claims.iat <= 1800
      && claims.exp * 1000 <= Date.now() + 1830_000) {
      return claims.exp * 1000
    }
  } catch {
    // 损坏或不符合契约的本地凭证交由调用方清理，不能恢复成已登录状态。
  }
  throw new Error('登录凭证无效，请重新登录。')
}
