import {expect,it} from 'vitest';
import {authIdentityKey,reducerOutboxScope,sameAuthTokenIdentity,localIdentityFromAuthResult,replicaDirectiveFromAuthResult,artifactHashFromAuthResult,developerSessionTokenFromAuthResult} from './client-auth-wire.js';
const jwt=(payload:unknown)=>'synthetic.'+btoa(JSON.stringify(payload))+'.signature';
it('shares the exact legacy identity namespace across token and explicit-hint restoration',()=>{
 const hint={project:'project',tenant:'tenant',identity:{sub:'account',iss:'issuer'}};
 const token={project:'project',tenant:'tenant',token:jwt({sub:'account',iss:'issuer'})};
 expect(authIdentityKey(token)).toBe('project\0tenant\0issuer\0account');
 expect(sameAuthTokenIdentity(hint,token)).toBe(true);
 expect(reducerOutboxScope('ws://private.test/ws',hint,'random')).toBe(reducerOutboxScope('ws://private.test/ws',token,'other'));
 expect(reducerOutboxScope('ws://private.test/ws',hint,'random')).toBe('identity\0ws://private.test/ws\0project\0tenant\0issuer\0account');
 expect(sameAuthTokenIdentity(hint,{...token,tenant:'other'})).toBe(false);
});
it('isolates conflicting credentials, opaque sessions and anonymous deployments',()=>{
 const auth={project:'p',tenant:'t',identity:{sub:'expected'},token:jwt({sub:'different'})};
 expect(authIdentityKey(auth)).toBe('');expect(reducerOutboxScope('ws://private.test',auth,'session')).toBe('ephemeral-auth\0ws://private.test\0session');
 expect(reducerOutboxScope('ws://private.test',{token:'opaque'},'one')).not.toBe(reducerOutboxScope('ws://private.test',{token:'opaque'},'two'));
 expect(reducerOutboxScope('ws://private.test',{project:'p',tenant:'one'},'unused')).not.toBe(reducerOutboxScope('ws://private.test',{project:'p',tenant:'two'},'unused'));
 expect(authIdentityKey({token:'invalid',identity:{sub:'account'}})).toBe('\0\0\0account');
});
it('parses only an admitted directive and clones authoritative local identity',()=>{
 expect(replicaDirectiveFromAuthResult({replica:{protocolVersion:2,scope:'s',visibilityScope:'v',epoch:'e'}})).toBeUndefined();
 const result={replica:{protocolVersion:1,scope:'s',visibilityScope:'v',epoch:'e'},localIdentity:{auth:{account:{id:'a'}},tenant:{id:'t'},member:{id:'m',accountId:'a',permissions:{manage:true}}}};
 expect(replicaDirectiveFromAuthResult(result)).toEqual(result.replica);
 const identity=localIdentityFromAuthResult(result)!;identity.member!.permissions.manage=false;expect(result.localIdentity.member.permissions.manage).toBe(true);
 expect(localIdentityFromAuthResult({localIdentity:{auth:{account:{id:'a'}}}})).toBeUndefined();
 expect(artifactHashFromAuthResult({artifactHash:'module'})).toBe('module');expect(artifactHashFromAuthResult({artifactHash:1})).toBeUndefined();
 expect(developerSessionTokenFromAuthResult({developerSessionToken:'gvx_dev_synthetic'})).toBe('gvx_dev_synthetic');expect(developerSessionTokenFromAuthResult({developerSessionToken:'ordinary-token'})).toBeUndefined();
});
